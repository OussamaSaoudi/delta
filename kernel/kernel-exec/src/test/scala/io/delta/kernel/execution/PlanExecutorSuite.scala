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

import java.util.Optional
import java.util.concurrent.{Callable, CompletableFuture}
import java.util.concurrent.{Executors, TimeoutException, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.data.{FilteredColumnarBatch, Row}
import io.delta.kernel.data.FilteredColumnarBatch.Lifetime
import io.delta.kernel.engine.ExpressionHandler
import io.delta.kernel.expressions.{Column, Expression, ExpressionEvaluator}
import io.delta.kernel.expressions.{Predicate, PredicateEvaluator}
import io.delta.kernel.internal.data.{GenericRow, RowBackedColumnarBatch}
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.plans.{Filter, PlanNode, Project, ScanFile, ScanJson, UnionAll, Values}
import io.delta.kernel.test.{BaseMockExpressionHandler, BaseMockJsonHandler}
import io.delta.kernel.test.{MockEngineUtils, VectorTestUtils}
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.{CloseableIterable, CloseableIterator}

import org.scalatest.funsuite.AnyFunSuite

class PlanExecutorSuite extends AnyFunSuite with MockEngineUtils with VectorTestUtils {
  private val schema = new StructType().add("id", IntegerType.INTEGER, false)

  test("execute Values, Filter, Project, and UnionAll") {
    val handler = new TrackingExpressionHandler()
    val filtered = new Filter(valuesPlan(1, 2, 3), new Predicate("IS_NOT_NULL", new Column("id")))
    val projected = new Project(filtered, new Column("id"), schema)
    val root = new UnionAll(Seq(projected, valuesPlan(4)).asJava)

    withExecutor(handler) { executor =>
      assert(collectInts(executor.execute(root)) === Seq(1, 3, 4))
    }
    assert(handler.predicateBindings === 1)
    assert(handler.expressionBindings === 1)
    assert(handler.evaluations === 1)
  }

  test("the same plan object executes once for multiple consumers") {
    val handler = new TrackingExpressionHandler()
    val shared = new Project(valuesPlan(1, 2), new Column("id"), schema)
    val root = new UnionAll(Seq[PlanNode](shared, shared).asJava)

    withExecutor(handler) { executor =>
      assert(collectInts(executor.execute(root)) === Seq(1, 2, 1, 2))
    }
    assert(handler.expressionBindings === 1)
    assert(handler.evaluations === 1)
  }

  test("structurally equal plan objects execute independently") {
    val handler = new TrackingExpressionHandler()
    val input = valuesPlan(1, 2)
    val left = new Project(input, new Column("id"), schema)
    val right = new Project(input, new Column("id"), schema)
    val root = new UnionAll(Seq[PlanNode](left, right).asJava)

    withExecutor(handler) { executor =>
      assert(collectInts(executor.execute(root)) === Seq(1, 2, 1, 2))
    }
    assert(handler.expressionBindings === 2)
    assert(handler.evaluations === 2)
  }

  test("an evaluator declares lifetime independently for each output batch") {
    val handler = new TrackingExpressionHandler(Seq(Lifetime.OWNED, Lifetime.BORROWED))
    val union = new UnionAll(Seq[PlanNode](valuesPlan(1), valuesPlan(2)).asJava)
    val project = new Project(union, new Column("id"), schema)

    withExecutor(handler) { executor =>
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
    val handler = new TrackingExpressionHandler()
    val json = new BaseMockJsonHandler {
      var opens = 0

      override def readJsonFiles(
          scan: ScanJson): CloseableIterator[FilteredColumnarBatch] = {
        opens += 1
        new TrackingIterator(Seq.empty)
      }
    }
    def scan: ScanJson = new ScanJson(
      Seq.empty[ScanFile].asJava,
      Optional.empty(),
      Seq.empty[String].asJava,
      schema)
    val root = new UnionAll(Seq[PlanNode](scan, scan).asJava)
    val cache = new PlanResultCache(4, Long.MaxValue)
    val result = new PlanExecutor(
      mockEngine(jsonHandler = json, expressionHandler = handler),
      cache).execute(root)
    try {
      assert(json.opens === 2)
      assert(!result.hasNext)
      assert(json.opens === 2)
    } finally {
      result.close()
      cache.close()
    }
  }

  test("execute uses a completed structural cache hit") {
    val cache = new PlanResultCache(4, Long.MaxValue)
    val key = valuesPlan(1)
    cache.put(key, ownedResult(9), 1)
    val engine = mockEngine(expressionHandler = new TrackingExpressionHandler)
    val executor = new PlanExecutor(engine, cache)
    try {
      assert(collectInts(executor.execute(valuesPlan(1))) === Seq(9))
    } finally {
      cache.close()
    }
  }

  test("an in-flight cache hit waits only on first pull") {
    val cache = new PlanResultCache(4, Long.MaxValue)
    val pending = new CompletableFuture[CloseableIterator[FilteredColumnarBatch]]()
    cache.prefetch(valuesPlan(1), pending, 1)
    val engine = mockEngine(expressionHandler = new TrackingExpressionHandler)
    val executor = new PlanExecutor(engine, cache)
    val result = executor.execute(valuesPlan(1))
    val worker = Executors.newSingleThreadExecutor()
    try {
      val pull = worker.submit(new Callable[java.lang.Boolean] {
        override def call(): java.lang.Boolean = result.hasNext
      })
      intercept[TimeoutException] {
        pull.get(100, TimeUnit.MILLISECONDS)
      }
      pending.complete(Utils.singletonCloseableIterator(batch(7, Lifetime.OWNED)))
      assert(pull.get(5, TimeUnit.SECONDS))
      assert(result.next().getData.getColumnVector(0).getInt(0) === 7)
    } finally {
      result.close()
      worker.shutdownNow()
      cache.close()
    }
  }

  test("first put wins") {
    val cache = new PlanResultCache(4, Long.MaxValue)
    val plan = valuesPlan(1)
    val first = new TrackingIterable(Seq(batch(1, Lifetime.OWNED)))
    val second = new TrackingIterable(Seq(batch(2, Lifetime.OWNED)))
    try {
      cache.put(plan, first, 1)
      cache.put(plan, second, 1)
      assert(second.closed)
      assert(collectInts(cache.get(plan)) === Seq(1))
    } finally {
      cache.close()
    }
  }

  test("a failed future is removed and may be retried") {
    val cache = new PlanResultCache(4, Long.MaxValue)
    val plan = valuesPlan(1)
    val failed = new CompletableFuture[CloseableIterator[FilteredColumnarBatch]]()
    try {
      cache.prefetch(plan, failed, 1)
      val observed = cache.get(plan)
      failed.completeExceptionally(new IllegalStateException("boom"))
      assert(intercept[IllegalStateException](observed.hasNext()).getMessage === "boom")
      assert(cache.get(plan) == null)

      cache.put(plan, ownedResult(3), 1)
      assert(collectInts(cache.get(plan)) === Seq(3))
    } finally {
      cache.close()
    }
  }

  test("entry eviction is insertion-order FIFO") {
    val cache = new PlanResultCache(2, Long.MaxValue)
    val first = valuesPlan(1)
    val second = valuesPlan(2)
    val third = valuesPlan(3)
    try {
      cache.put(first, ownedResult(1), 1)
      cache.put(second, ownedResult(2), 1)
      cache.get(first).close()
      cache.put(third, ownedResult(3), 1)

      assert(cache.get(first) == null)
      cache.get(second).close()
      cache.get(third).close()
    } finally {
      cache.close()
    }
  }

  test("a prefetched result is claimed once") {
    val cache = new PlanResultCache(2, Long.MaxValue)
    val plan = valuesPlan(1)
    try {
      cache.prefetch(plan, Utils.singletonCloseableIterator(batch(8, Lifetime.OWNED)), 1)
      val claimed = cache.get(plan)
      assert(cache.get(plan) == null)
      assert(collectInts(claimed) === Seq(8))
    } finally {
      cache.close()
    }
  }

  test("a shared plan consumes one prefetched cursor") {
    val cache = new PlanResultCache(2, Long.MaxValue)
    val shared = valuesPlan(1)
    val root = new UnionAll(Seq[PlanNode](shared, shared).asJava)
    val prefetched = new TrackingIterator(Seq(batch(8, Lifetime.OWNED)))
    try {
      cache.prefetch(shared, prefetched, 1)
      val executor = new PlanExecutor(
        mockEngine(expressionHandler = new TrackingExpressionHandler),
        cache)
      assert(collectInts(executor.execute(root)) === Seq(8, 8))
      assert(prefetched.nextCalls === 1)
    } finally {
      cache.close()
    }
  }

  test("closing a future result before publication cancels the handoff") {
    val cache = new PlanResultCache(2, Long.MaxValue)
    val plan = valuesPlan(1)
    val pending = new CompletableFuture[CloseableIterator[FilteredColumnarBatch]]()
    val published = new TrackingIterator(Seq(batch(8, Lifetime.OWNED)))
    try {
      cache.prefetch(plan, pending, 1)
      cache.get(plan).close()
      assert(!pending.complete(published))
      published.close()
      assert(published.closed)
    } finally {
      cache.close()
    }
  }

  test("eviction waits for replayable cursors to close") {
    val cache = new PlanResultCache(1, Long.MaxValue)
    val first = new TrackingIterable(Seq(batch(1, Lifetime.OWNED)))
    try {
      cache.put(valuesPlan(1), first, 1)
      val pinned = cache.get(valuesPlan(1))
      cache.put(valuesPlan(2), ownedResult(2), 1)
      assert(!first.closed)
      assert(collectInts(pinned) === Seq(1))
      assert(first.closed)
    } finally {
      cache.close()
    }
  }

  test("disabled cache misses and closes offered results") {
    val cache = PlanResultCache.disabled()
    val offered = new TrackingIterable(Seq(batch(1, Lifetime.OWNED)))
    try {
      cache.put(valuesPlan(1), offered, 1)
      assert(offered.closed)
      assert(cache.get(valuesPlan(1)) == null)
    } finally {
      cache.close()
    }
  }

  private def withExecutor(handler: ExpressionHandler)(testCode: PlanExecutor => Unit): Unit = {
    val cache = new PlanResultCache(16, Long.MaxValue)
    try {
      testCode(new PlanExecutor(mockEngine(expressionHandler = handler), cache))
    } finally {
      cache.close()
    }
  }

  private def valuesPlan(values: Int*): Values =
    new Values(schema, values.map(row).asJava)

  private def row(value: Int): Row =
    GenericRow.fromOwnedValues(schema, Array[AnyRef](Int.box(value)))

  private def batch(value: Int, lifetime: Lifetime): FilteredColumnarBatch =
    new FilteredColumnarBatch(
      new RowBackedColumnarBatch(schema, Seq(row(value)).asJava),
      Optional.empty(),
      lifetime)

  private def ownedResult(value: Int): CloseableIterable[FilteredColumnarBatch] =
    new TrackingIterable(Seq(batch(value, Lifetime.OWNED)))

  private def collectInts(result: CloseableIterator[FilteredColumnarBatch]): Seq[Int] = {
    val values = ArrayBuffer.empty[Int]
    try {
      while (result.hasNext) {
        val rows = result.next().getRows
        try {
          while (rows.hasNext) values += rows.next().getInt(0)
        } finally {
          rows.close()
        }
      }
    } finally {
      result.close()
    }
    values.toSeq
  }

  private final class TrackingExpressionHandler(
      outputLifetimes: Seq[Lifetime] = Seq.empty)
      extends BaseMockExpressionHandler {
    var predicateBindings = 0
    var expressionBindings = 0
    var evaluations = 0

    override def getPredicateEvaluator(
        inputSchema: StructType,
        predicate: Predicate): PredicateEvaluator = {
      predicateBindings += 1
      new PredicateEvaluator {
        override def eval(input: FilteredColumnarBatch): FilteredColumnarBatch = {
          val data = input.getData
          val selected = (0 until data.getSize).map { rowId =>
            Boolean.box(data.getColumnVector(0).getInt(rowId) % 2 == 1)
          }
          input.withSelectionVector(booleanVector(selected), Lifetime.BORROWED)
        }
      }
    }

    override def getEvaluator(
        inputSchema: StructType,
        expression: Expression,
        outputSchema: StructType): ExpressionEvaluator = {
      expressionBindings += 1
      new ExpressionEvaluator {
        override def eval(input: FilteredColumnarBatch): FilteredColumnarBatch = {
          val index = evaluations
          evaluations += 1
          val lifetime = outputLifetimes.lift(index).getOrElse(input.getLifetime)
          input.withData(input.getData, lifetime)
        }

        override def close(): Unit = {}
      }
    }
  }

  private final class TrackingIterator(batches: Seq[FilteredColumnarBatch])
      extends CloseableIterator[FilteredColumnarBatch] {
    private val iterator = batches.iterator
    var closed = false
    var nextCalls = 0

    override def hasNext: Boolean = iterator.hasNext

    override def next(): FilteredColumnarBatch = {
      nextCalls += 1
      iterator.next()
    }

    override def close(): Unit = closed = true
  }

  private final class TrackingIterable(batches: Seq[FilteredColumnarBatch])
      extends CloseableIterable[FilteredColumnarBatch] {
    var closed = false

    override def iterator(): CloseableIterator[FilteredColumnarBatch] = {
      assert(!closed)
      new TrackingIterator(batches)
    }

    override def close(): Unit = closed = true
  }
}
