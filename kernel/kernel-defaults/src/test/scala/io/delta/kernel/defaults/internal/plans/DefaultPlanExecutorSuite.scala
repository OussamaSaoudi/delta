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
package io.delta.kernel.defaults.internal.plans

import java.lang.{Long => LongJ}
import java.util.Optional
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ColumnarBatch, Row}
import io.delta.kernel.engine.FileReadResult
import io.delta.kernel.expressions.{Column, Expression, Literal, Predicate, StructExpression}
import io.delta.kernel.internal.actions.DeletionVectorDescriptor
import io.delta.kernel.internal.plans._
import io.delta.kernel.test.{BaseMockJsonHandler, BaseMockParquetHandler, MockEngineUtils}
import io.delta.kernel.types.{LongType, StringType, StructType}
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.scalatest.funsuite.AnyFunSuite

class DefaultPlanExecutorSuite extends AnyFunSuite with MockEngineUtils {
  private val idSchema = new StructType().add("id", LongType.LONG, false)

  private def row(schema: StructType, values: AnyRef*): Row =
    PlanTestUtils.row(schema, values: _*)

  private def values(schema: StructType, rows: Row*): Values =
    new Values(schema, rows.asJava)

  private def node(operator: Operator, inputs: Int*): PlanNode =
    new PlanNode(operator, inputs.map(Int.box).asJava)

  test("streams a typed pipeline and dispatches every in-memory operator") {
    val projectedSchema = new StructType().add("value", LongType.LONG, false)
    val project = new Project(
      new StructExpression(Seq[Expression](new Column("id")).asJava),
      projectedSchema)
    val join = new SemiJoin(
      false,
      Seq(new Column("value")).asJava,
      Seq(new Column("value")).asJava)
    val filter = new Filter(
      new Predicate(">", new Column("value"), Literal.ofLong(1)))
    val aggregate = Aggregate
      .ungrouped(projectedSchema)
      .max(new Column("value"))
      .build()
    val aggregateSchema = aggregate.getSchema

    val plan = new Plan(Seq(
      node(values(idSchema, row(idSchema, LongJ.valueOf(1)), row(idSchema, LongJ.valueOf(2)))),
      node(project, 0),
      node(values(projectedSchema, row(projectedSchema, LongJ.valueOf(2)))),
      node(join, 1, 2),
      node(filter, 3),
      node(aggregate, 4),
      node(values(aggregateSchema, row(aggregateSchema, LongJ.valueOf(3)))),
      node(UnionAll.UNION_ALL, 5, 6)).asJava)

    PlanTestUtils.assertRows(
      DefaultPlanExecutor.execute(plan, mockEngine()),
      Seq(row(aggregateSchema, LongJ.valueOf(2)), row(aggregateSchema, LongJ.valueOf(3))))
  }

  test("materializes a shared DAG node once so repeated inputs are replayable") {
    val source = values(idSchema, row(idSchema, LongJ.valueOf(7)))
    val plan = new Plan(Seq(
      node(source),
      node(UnionAll.UNION_ALL, 0, 0)).asJava)

    PlanTestUtils.assertRows(
      DefaultPlanExecutor.execute(plan, mockEngine()),
      Seq(row(idSchema, LongJ.valueOf(7)), row(idSchema, LongJ.valueOf(7))))
  }

  test("dispatches an empty Load without introducing an Arrow boundary") {
    val metadataSchema = new StructType()
      .add("path", StringType.STRING, false)
      .add("size", LongType.LONG, true)
      .add("num_records", LongType.LONG, true)
      .add("dv", DeletionVectorDescriptor.READ_SCHEMA, true)
    val fileMeta = new LoadColumnFileMeta(
      new Column("path"),
      new Column("size"),
      new Column("num_records"))
    val load = new Load(
      idSchema,
      FileType.PARQUET,
      fileMeta,
      new Column("dv"))
    val plan = new Plan(Seq(
      node(values(metadataSchema)),
      node(load, 0)).asJava)

    PlanTestUtils.assertRows(DefaultPlanExecutor.execute(plan, mockEngine()), Seq.empty)
  }

  test("prefetches all reachable leaf scans before an eager aggregate blocks") {
    val bothStarted = new CountDownLatch(2)
    val parquetFile = new ScanFile(FileStatus.of("file:///table/data.parquet", 10, 1))
    val jsonFile = new ScanFile(FileStatus.of("file:///table/data.json", 10, 1))
    val parquet = new BarrierParquetHandler(
      bothStarted,
      PlanTestUtils.columnarBatch(idSchema, Seq(row(idSchema, LongJ.valueOf(1)))))
    val json = new BarrierJsonHandler(
      bothStarted,
      PlanTestUtils.columnarBatch(idSchema, Seq(row(idSchema, LongJ.valueOf(2)))))
    val aggregate = Aggregate.ungrouped(idSchema).max(new Column("id")).build()
    val plan = new Plan(Seq(
      node(new ScanParquet(Seq(parquetFile).asJava, Seq.empty[String].asJava, idSchema)),
      node(aggregate, 0),
      node(new ScanJson(Seq(jsonFile).asJava, Seq.empty[String].asJava, idSchema)),
      node(aggregate, 2),
      node(UnionAll.UNION_ALL, 1, 3)).asJava)
    val ioExecutor = Executors.newFixedThreadPool(4)

    try {
      val result = DefaultPlanExecutor.execute(
        plan,
        mockEngine(jsonHandler = json, parquetHandler = parquet),
        ioExecutor)
      val outputSchema = aggregate.getSchema
      PlanTestUtils.assertRows(
        result,
        Seq(row(outputSchema, LongJ.valueOf(1)), row(outputSchema, LongJ.valueOf(2))))
      assert(!ioExecutor.isShutdown, "the caller owns an explicit ExecutorService")
    } finally {
      ioExecutor.shutdownNow()
      assert(ioExecutor.awaitTermination(10, TimeUnit.SECONDS))
    }
  }

  test("rejects non-positive public I/O parallelism") {
    val plan = new Plan(Seq(node(values(idSchema))).asJava)
    for (parallelism <- Seq(0, -1)) {
      val failure = intercept[IllegalArgumentException] {
        DefaultPlanExecutor.execute(plan, mockEngine(), parallelism)
      }
      assert(failure.getMessage.contains("must be positive"))
    }
  }

  test("rejects unsupported operators before starting leaf I/O") {
    var opened = false
    val handler = new BaseMockParquetHandler {
      override def readParquetFiles(
          files: CloseableIterator[FileStatus],
          physicalSchema: StructType,
          predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = {
        opened = true
        new TrackingIterator(Seq.empty)
      }
    }
    val unsupported = new Operator {
      override def getOutputSchema(inputSchemas: java.util.List[StructType]): StructType =
        inputSchemas.get(0)
    }
    val scanFile = new ScanFile(FileStatus.of("file:///table/data.parquet", 10, 1))
    val plan = new Plan(Seq(
      node(new ScanParquet(Seq(scanFile).asJava, Seq.empty[String].asJava, idSchema)),
      node(unsupported, 0)).asJava)

    assertThrows[UnsupportedOperationException] {
      DefaultPlanExecutor.execute(plan, mockEngine(parquetHandler = handler))
    }
    assert(!opened)
  }

  test("exhaustion closes readers without an explicit close call") {
    val reader = new TrackingIterator[FileReadResult](Seq.empty)
    val handler = new BaseMockParquetHandler {
      override def readParquetFiles(
          files: CloseableIterator[FileStatus],
          physicalSchema: StructType,
          predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = reader
    }
    val scanFile = new ScanFile(FileStatus.of("file:///table/data.parquet", 10, 1))
    val plan = new Plan(Seq(
      node(new ScanParquet(Seq(scanFile).asJava, Seq.empty[String].asJava, idSchema))).asJava)

    assert(!DefaultPlanExecutor.execute(plan, mockEngine(parquetHandler = handler)).hasNext)
    assert(reader.closeCalls === 1)
  }

  private final class BarrierParquetHandler(
      barrier: CountDownLatch,
      batch: ColumnarBatch)
      extends BaseMockParquetHandler {
    override def readParquetFiles(
        files: CloseableIterator[FileStatus],
        physicalSchema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = {
      val file = files.next()
      files.close()
      new BarrierIterator(
        Seq(new FileReadResult(batch, file.getPath)),
        barrier)
    }
  }

  private final class BarrierJsonHandler(
      barrier: CountDownLatch,
      batch: ColumnarBatch)
      extends BaseMockJsonHandler {
    override def readJsonFiles(
        files: CloseableIterator[FileStatus],
        physicalSchema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
      files.close()
      new BarrierIterator(Seq(batch), barrier)
    }
  }

  private final class BarrierIterator[T](values: Seq[T], barrier: CountDownLatch)
      extends CloseableIterator[T] {
    private var index = 0
    private var started = false

    override def hasNext: Boolean = {
      if (!started) {
        started = true
        barrier.countDown()
        assert(barrier.await(5, TimeUnit.SECONDS), "leaf scans did not start together")
      }
      index < values.size
    }

    override def next(): T = {
      val value = values(index)
      index += 1
      value
    }

    override def close(): Unit = {}
  }
}
