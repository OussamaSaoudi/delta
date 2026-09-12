/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.kernel.execution

import java.util.{Collections, HashMap, Optional}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.data.{ColumnarBatch, ColumnVector, Row}
import io.delta.kernel.data.ColumnarBatch.Lifetime
import io.delta.kernel.execution.PlanEngine.BatchEvaluator
import io.delta.kernel.expressions.{Column, Expression, ExpressionEvaluator, Predicate, PredicateEvaluator, StructExpression}
import io.delta.kernel.internal.data.{GenericColumnVector, GenericRow, RowBackedColumnarBatch}
import io.delta.kernel.plans.Agg
import io.delta.kernel.plans.PlanNode
import io.delta.kernel.plans.PlanNode.{Aggregate, Cte, FileScan, Filter, Project, UnionAll, Values}
import io.delta.kernel.test.{BaseMockExpressionHandler, MockEngineUtils}
import io.delta.kernel.types.{BooleanType, DataType, IntegerType, LongType, StructType}
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class PlanExecutorSuite extends AnyFunSuite with MockEngineUtils {
  private val schema = new StructType().add("id", IntegerType.INTEGER, false)

  test("execute Values, Filter, Project, and UnionAll") {
    val engine = new TrackingPlanEngine
    val filtered = new Filter(
      valuesPlan(1, 2, 3),
      new Predicate("IS_NOT_NULL", new io.delta.kernel.expressions.Column("id")))
    val projected = new Project(filtered, new io.delta.kernel.expressions.Column("id"), schema)
    val root = new UnionAll(Seq[PlanNode](projected, valuesPlan(4)).asJava)

    withExecutor(engine) { executor =>
      assert(collectInts(executor.execute(root)) === Seq(1, 3, 4))
    }
    assert(engine.predicateBindings === 1)
    assert(engine.expressionBindings === 1)
    assert(engine.evaluations === 1)
    assert(engine.evaluatorCloses === 2)
  }

  test("Engine constructor evaluates filters and struct projects through its handlers") {
    var expressionClosed = false
    val expressions = new BaseMockExpressionHandler {
      override def getPredicateEvaluator(
          inputSchema: StructType,
          predicate: Predicate): PredicateEvaluator =
        new PredicateEvaluator {
          override def eval(
              input: ColumnarBatch,
              selection: Optional[ColumnVector]): ColumnVector = {
            val selected = (0 until input.getSize).map { rowId =>
              Boolean.box(input.getColumnVector(0).getInt(rowId) % 2 == 1)
            }
            new GenericColumnVector(selected.asJava, BooleanType.BOOLEAN)
          }
        }

      override def getEvaluator(
          inputSchema: StructType,
          expression: Expression,
          outputType: DataType): ExpressionEvaluator =
        new ExpressionEvaluator {
          override def eval(input: ColumnarBatch): ColumnVector = input.getColumnVector(0)
          override def close(): Unit = expressionClosed = true
        }
    }
    val filtered = new Filter(
      valuesPlan(1, 2, 3),
      new Predicate("IS_NOT_NULL", new io.delta.kernel.expressions.Column("id")))
    val projected = new Project(
      filtered,
      new StructExpression(
        Collections.singletonList[Expression](new io.delta.kernel.expressions.Column("id"))),
      schema)
    val cache = new PlanResultCache(4, Long.MaxValue)
    try {
      val result = new PlanExecutor(mockEngine(expressionHandler = expressions), cache)
        .execute(projected)
      try {
        val batch = result.next()
        assert(batch.getLifetime === Lifetime.BORROWED)
        assert(ints(batch) === Seq(1, 3))
      } finally {
        result.close()
      }
      assert(expressionClosed)
    } finally {
      cache.close()
    }
  }

  test("structurally equal nodes execute independently") {
    val engine = new TrackingPlanEngine
    val input = valuesPlan(1, 2)
    val left = new Project(input, new io.delta.kernel.expressions.Column("id"), schema)
    val right = new Project(input, new io.delta.kernel.expressions.Column("id"), schema)
    val root = new UnionAll(Seq[PlanNode](left, right).asJava)

    withExecutor(engine) { executor =>
      assert(collectInts(executor.execute(root)) === Seq(1, 2, 1, 2))
    }
    assert(engine.expressionBindings === 2)
    assert(engine.evaluations === 2)
  }

  test("CTE references materialize one engine-native result") {
    val engine = new TrackingPlanEngine
    val shared = new Cte(
      7L,
      new Project(valuesPlan(1, 2), new Column("id"), schema))
    val root = new UnionAll(Seq[PlanNode](shared, shared).asJava)

    withExecutor(engine) { executor =>
      assert(collectInts(executor.execute(root)) === Seq(1, 2, 1, 2))
    }
    assert(engine.expressionBindings === 1)
    assert(engine.evaluations === 1)
  }

  test("an evaluator chooses the lifetime of each output batch") {
    val engine = new TrackingPlanEngine(Seq(Lifetime.OWNED, Lifetime.BORROWED))
    val union = new UnionAll(Seq[PlanNode](valuesPlan(1), valuesPlan(2)).asJava)
    val project = new Project(
      union,
      new io.delta.kernel.expressions.Column("id"),
      schema)

    withExecutor(engine) { executor =>
      val result = executor.execute(project)
      try {
        assert(result.next().getLifetime === Lifetime.OWNED)
        assert(result.next().getLifetime === Lifetime.BORROWED)
      } finally {
        result.close()
      }
    }
  }

  test("execute opens every static scan before the first pull") {
    var opens = 0
    val engine = new TrackingPlanEngine(openScan = _ => {
      opens += 1
      new TrackingIterator(Seq.empty)
    })
    def scan: FileScan = FileScan.json(
      Seq.empty.asJava,
      Optional.empty(),
      Seq.empty.asJava,
      schema)
    val root = new UnionAll(Seq[PlanNode](scan, scan).asJava)
    val cache = new PlanResultCache(4, Long.MaxValue)
    val result = new PlanExecutor(engine, cache).execute(root)
    try {
      assert(opens === 2)
      assert(!result.hasNext)
    } finally {
      result.close()
      cache.close()
    }
  }

  test("SUM and COUNT use LONG results for INT and LONG inputs") {
    val inputSchema = new StructType()
      .add("group", IntegerType.INTEGER, false)
      .add("intValue", IntegerType.INTEGER, true)
      .add("longValue", LongType.LONG, true)
    val outputSchema = new StructType()
      .add("group", IntegerType.INTEGER, false)
      .add("intSum", LongType.LONG, true)
      .add("longSum", LongType.LONG, true)
      .add("nonNullInts", LongType.LONG, false)
      .add("rows", LongType.LONG, false)
    val input = new Values(
      inputSchema,
      Seq(
        row(inputSchema, Int.box(1), Int.box(1), Long.box(10)),
        row(inputSchema, Int.box(1), Int.box(2), null),
        row(inputSchema, Int.box(1), null, Long.box(30)),
        row(inputSchema, Int.box(2), null, null)).asJava)
    val aggregate = new Aggregate(
      input,
      Seq[Expression](new Column("group")).asJava,
      Seq(
        Agg.sum(new Column("intValue"), IntegerType.INTEGER),
        Agg.sum(new Column("longValue"), LongType.LONG),
        Agg.count(new Column("intValue"), IntegerType.INTEGER),
        Agg.countStar()).asJava,
      outputSchema)

    val cache = new PlanResultCache(4, Long.MaxValue)
    val result = new PlanExecutor(mockEngine(expressionHandler = columnExpressions), cache)
      .execute(aggregate)
    try {
      val batch = result.next()
      assert(!result.hasNext)
      val rows = batch.getRows
      try {
        val first = rows.next()
        assert((
          first.getInt(0),
          first.getLong(1),
          first.getLong(2),
          first.getLong(3),
          first.getLong(4)) === (1, 3L, 40L, 2L, 3L))
        val second = rows.next()
        assert(second.getInt(0) === 2)
        assert(second.isNullAt(1))
        assert(second.isNullAt(2))
        assert(second.getLong(3) === 0L)
        assert(second.getLong(4) === 1L)
        assert(!rows.hasNext)
      } finally {
        rows.close()
      }
    } finally {
      result.close()
      cache.close()
    }
  }

  test("global SUM is null and COUNT is zero for empty input") {
    val inputSchema = new StructType().add("value", LongType.LONG, true)
    val outputSchema = new StructType()
      .add("sum", LongType.LONG, true)
      .add("count", LongType.LONG, false)
      .add("rows", LongType.LONG, false)
    val aggregate = new Aggregate(
      new Values(inputSchema, Seq.empty[Row].asJava),
      Seq.empty[Expression].asJava,
      Seq(
        Agg.sum(new Column("value"), LongType.LONG),
        Agg.count(new Column("value"), LongType.LONG),
        Agg.countStar()).asJava,
      outputSchema)

    val cache = new PlanResultCache(4, Long.MaxValue)
    val result = new PlanExecutor(mockEngine(expressionHandler = columnExpressions), cache)
      .execute(aggregate)
    try {
      val rows = result.next().getRows
      try {
        val row = rows.next()
        assert(row.isNullAt(0))
        assert(row.getLong(1) === 0L)
        assert(row.getLong(2) === 0L)
        assert(!rows.hasNext)
        assert(!result.hasNext)
      } finally {
        rows.close()
      }
    } finally {
      result.close()
      cache.close()
    }
  }

  private def withExecutor(engine: PlanEngine)(testCode: PlanExecutor => Unit): Unit = {
    val cache = new PlanResultCache(16, Long.MaxValue)
    try {
      testCode(new PlanExecutor(engine, cache))
    } finally {
      cache.close()
    }
  }

  private def valuesPlan(values: Int*): Values =
    new Values(schema, values.map(row).asJava)

  private def row(value: Int): Row =
    new GenericRow(
      schema,
      Collections.singletonMap[Integer, Object](Int.box(0), Int.box(value)))

  private def row(schema: StructType, values: Object*): Row = {
    val present = new HashMap[Integer, Object]
    values.zipWithIndex.foreach { case (value, ordinal) =>
      if (value != null) present.put(Int.box(ordinal), value)
    }
    new GenericRow(schema, present)
  }

  private val columnExpressions = new BaseMockExpressionHandler {
    override def getEvaluator(
        inputSchema: StructType,
        expression: Expression,
        outputType: DataType): ExpressionEvaluator = {
      val names = expression.asInstanceOf[Column].getNames
      require(names.length === 1)
      val ordinal = inputSchema.indexOf(names.head)
      require(ordinal >= 0)
      new ExpressionEvaluator {
        override def eval(input: ColumnarBatch): ColumnVector =
          input.getColumnVector(ordinal)

        override def close(): Unit = ()
      }
    }
  }

  private def collectInts(result: CloseableIterator[ColumnarBatch]): Seq[Int] = {
    val values = ArrayBuffer.empty[Int]
    try {
      while (result.hasNext) values ++= ints(result.next())
    } finally {
      result.close()
    }
    values.toSeq
  }

  private def ints(batch: ColumnarBatch): Seq[Int] = {
    val rows = batch.getRows
    val values = ArrayBuffer.empty[Int]
    try {
      while (rows.hasNext) values += rows.next().getInt(0)
    } finally {
      rows.close()
    }
    values.toSeq
  }

  private final class TrackingPlanEngine(
      outputLifetimes: Seq[Lifetime] = Seq.empty,
      openScan: FileScan => CloseableIterator[ColumnarBatch] =
        _ => new TrackingIterator(Seq.empty)) extends PlanEngine {
    var predicateBindings = 0
    var expressionBindings = 0
    var evaluations = 0
    var evaluatorCloses = 0

    override def scan(scan: FileScan): CloseableIterator[ColumnarBatch] = openScan(scan)

    override def filter(input: ColumnarBatch, keep: Array[Boolean]): ColumnarBatch =
      input.asInstanceOf[RowBackedColumnarBatch].filter(keep)

    override def retainRow(
        input: Row,
        schema: StructType): Row =
      throw new UnsupportedOperationException("This stateless test engine does not retain rows")

    override def retainValue(input: Row, ordinal: Int, schema: StructType): Row =
      throw new UnsupportedOperationException("This stateless test engine does not retain values")

    override def longValue(value: Long, schema: StructType): Row =
      throw new UnsupportedOperationException("This stateless test engine does not create values")

    override def appendColumns(
        input: ColumnarBatch,
        outputSchema: StructType,
        columns: java.util.List[java.util.List[Row]]): ColumnarBatch =
      throw new UnsupportedOperationException("This stateless test engine does not append columns")

    override def bind(inputSchema: StructType, predicate: Predicate): BatchEvaluator = {
      predicateBindings += 1
      new BatchEvaluator {
        override def eval(input: ColumnarBatch): ColumnarBatch = {
          val keep = Array.tabulate(input.getSize) { rowId =>
            input.getColumnVector(0).getInt(rowId) % 2 == 1
          }
          filter(input, keep)
        }

        override def close(): Unit = evaluatorCloses += 1
      }
    }

    override def bind(
        inputSchema: StructType,
        expression: Expression,
        outputSchema: StructType): BatchEvaluator = {
      expressionBindings += 1
      new BatchEvaluator {
        override def eval(input: ColumnarBatch): ColumnarBatch = {
          val lifetime = outputLifetimes.lift(evaluations).getOrElse(input.getLifetime)
          evaluations += 1
          val rows = input.getRows
          val output = ArrayBuffer.empty[Row]
          try {
            while (rows.hasNext) output += rows.next()
          } finally {
            rows.close()
          }
          new RowBackedColumnarBatch(input.getSchema, output.asJava, lifetime)
        }

        override def close(): Unit = evaluatorCloses += 1
      }
    }
  }

  private final class TrackingIterator(batches: Seq[ColumnarBatch])
      extends CloseableIterator[ColumnarBatch] {
    private val iterator = batches.iterator

    override def hasNext: Boolean = iterator.hasNext
    override def next(): ColumnarBatch = iterator.next()
    override def close(): Unit = {}
  }
}
