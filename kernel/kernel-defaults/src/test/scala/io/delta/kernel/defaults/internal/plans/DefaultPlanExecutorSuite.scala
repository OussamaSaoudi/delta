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
import java.util.function.LongSupplier

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ColumnarBatch, Row}
import io.delta.kernel.engine.FileReadResult
import io.delta.kernel.expressions.{Column, Expression, Literal, Predicate, StructExpression}
import io.delta.kernel.internal.actions.DeletionVectorDescriptor
import io.delta.kernel.internal.plans._
import io.delta.kernel.test.{BaseMockParquetHandler, MockEngineUtils}
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

  test("reports nesting-correct measurements using stable plan node identities") {
    val filter = new Filter(new Predicate(">", new Column("id"), Literal.ofLong(1)))
    val plan = new Plan(Seq(
      node(values(
        idSchema,
        row(idSchema, LongJ.valueOf(1)),
        row(idSchema, LongJ.valueOf(2)))),
      node(filter, 0)).asJava)
    val measurements = ArrayBuffer.empty[PlanExecutionObserver.Measurement]
    val observer = new PlanExecutionObserver {
      override def onMeasurement(measurement: PlanExecutionObserver.Measurement): Unit =
        measurements += measurement
    }

    val result = DefaultPlanExecutor.execute(plan, mockEngine(), observer)
    assert(measurements.isEmpty, "measurements are reported only when execution finishes")
    PlanTestUtils.assertRows(
      result,
      Seq(row(idSchema, LongJ.valueOf(2))))

    val byNodeAndPhase = measurements.map { measurement =>
      (measurement.getNodeIndex, measurement.getPhase) -> measurement
    }.toMap
    assert(measurements.map(_.getNodeIndex).toSet == Set(0, 1))
    assert(byNodeAndPhase((0, PlanExecutionObserver.Phase.NEXT)).getOperatorType == "Values")
    assert(byNodeAndPhase((1, PlanExecutionObserver.Phase.NEXT)).getOperatorType == "Filter")

    measurements.foreach { measurement =>
      assert(measurement.getInvocationCount > 0)
      assert(measurement.getInclusiveDurationNs >= measurement.getChildDurationNs)
      assert(
        measurement.getSelfDurationNs + measurement.getChildDurationNs ==
          measurement.getInclusiveDurationNs)
    }
    PlanExecutionObserver.Phase.values().foreach { phase =>
      assert(byNodeAndPhase((1, phase)).getChildDurationNs > 0)
    }

    val valuesNext = byNodeAndPhase((0, PlanExecutionObserver.Phase.NEXT))
    assert(valuesNext.getOutputBatchCount == 1)
    assert(valuesNext.getOutputPhysicalRowCount == 2)
    assert(valuesNext.getOutputKnownSelectedRowCount == 2)
    assert(valuesNext.getOutputUnknownSelectedBatchCount == 0)

    val filterNext = byNodeAndPhase((1, PlanExecutionObserver.Phase.NEXT))
    assert(filterNext.getOutputBatchCount == 1)
    assert(filterNext.getOutputPhysicalRowCount == 2)
    assert(filterNext.getOutputKnownSelectedRowCount == 0)
    assert(filterNext.getOutputUnknownSelectedBatchCount == 1)
  }

  test("NOOP observer preserves the uninstrumented execution path") {
    val plan = new Plan(Seq(
      node(values(idSchema, row(idSchema, LongJ.valueOf(7))))).asJava)

    PlanTestUtils.assertRows(
      DefaultPlanExecutor.execute(plan, mockEngine(), PlanExecutionObserver.NOOP),
      Seq(row(idSchema, LongJ.valueOf(7))))
  }

  test("computes inclusive, child, and self duration from nested calls") {
    val filter = new Filter(new Predicate(">", new Column("id"), Literal.ofLong(1)))
    val plan = new Plan(Seq(
      node(values(idSchema, row(idSchema, LongJ.valueOf(2)))),
      node(filter, 0)).asJava)
    val measurements = ArrayBuffer.empty[PlanExecutionObserver.Measurement]
    val observer = new PlanExecutionObserver {
      override def onMeasurement(measurement: PlanExecutionObserver.Measurement): Unit =
        measurements += measurement
    }
    val times = Iterator(0L, 2L, 5L, 10L)
    val clock = new LongSupplier {
      override def getAsLong: Long = times.next()
    }
    val instrumentation = new PlanExecutionInstrumentation(plan, observer, clock)

    instrumentation.start(1, PlanExecutionObserver.Phase.PREPARE)
    instrumentation.start(0, PlanExecutionObserver.Phase.PREPARE)
    instrumentation.stop()
    instrumentation.stop()
    instrumentation.finish()

    val byNode = measurements.map(measurement => measurement.getNodeIndex -> measurement).toMap
    assert(byNode(0).getInclusiveDurationNs == 3)
    assert(byNode(0).getChildDurationNs == 0)
    assert(byNode(0).getSelfDurationNs == 3)
    assert(byNode(1).getInclusiveDurationNs == 10)
    assert(byNode(1).getChildDurationNs == 3)
    assert(byNode(1).getSelfDurationNs == 7)
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

  test("fairly prefetches all reachable leaf scan groups before execution") {
    val bothStarted = new CountDownLatch(2)
    val firstFiles = (0 until 3).map(index =>
      new ScanFile(FileStatus.of(s"file:///table/a$index.parquet", 10, 1)))
    val secondFile = new ScanFile(FileStatus.of("file:///table/b0.parquet", 10, 1))
    val starts = ArrayBuffer.empty[String]
    val parquet = new BarrierParquetHandler(
      bothStarted,
      starts,
      (firstFiles :+ secondFile).zipWithIndex.map { case (file, index) =>
        file.getFileStatus.getPath -> PlanTestUtils.columnarBatch(
          idSchema,
          Seq(row(idSchema, LongJ.valueOf(index + 1))))
      }.toMap)
    val aggregate = Aggregate.ungrouped(idSchema).max(new Column("id")).build()
    val plan = new Plan(Seq(
      node(new ScanParquet(firstFiles.asJava, Seq.empty[String].asJava, idSchema)),
      node(aggregate, 0),
      node(new ScanParquet(Seq(secondFile).asJava, Seq.empty[String].asJava, idSchema)),
      node(aggregate, 2),
      node(UnionAll.UNION_ALL, 1, 3)).asJava)
    val ioExecutor = Executors.newFixedThreadPool(2)

    try {
      val result = DefaultPlanExecutor.execute(
        plan,
        mockEngine(parquetHandler = parquet),
        ioExecutor)
      val outputSchema = aggregate.getSchema
      PlanTestUtils.assertRows(
        result,
        Seq(row(outputSchema, LongJ.valueOf(3)), row(outputSchema, LongJ.valueOf(4))))
      assert(starts.synchronized(starts.take(2).toSet) === Set(
        firstFiles.head.getFileStatus.getPath,
        secondFile.getFileStatus.getPath))
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

  test("a caller-owned I/O executor survives early close and exhaustion") {
    val plan = new Plan(Seq(node(values(idSchema, row(idSchema, LongJ.valueOf(7))))).asJava)
    val ioExecutor = Executors.newFixedThreadPool(1)

    try {
      val closedEarly = DefaultPlanExecutor.execute(plan, mockEngine(), ioExecutor)
      closedEarly.close()
      assert(!ioExecutor.isShutdown)

      PlanTestUtils.assertRows(
        DefaultPlanExecutor.execute(plan, mockEngine(), ioExecutor),
        Seq(row(idSchema, LongJ.valueOf(7))))
      assert(!ioExecutor.isShutdown)
    } finally {
      ioExecutor.shutdownNow()
      assert(ioExecutor.awaitTermination(10, TimeUnit.SECONDS))
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

  test("does not register unreachable scan sources") {
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
    val scanFile = new ScanFile(FileStatus.of("file:///table/unreachable.parquet", 10, 1))
    val plan = new Plan(Seq(
      node(new ScanParquet(Seq(scanFile).asJava, Seq.empty[String].asJava, idSchema)),
      node(values(idSchema, row(idSchema, LongJ.valueOf(7))))).asJava)

    PlanTestUtils.assertRows(
      DefaultPlanExecutor.execute(plan, mockEngine(parquetHandler = handler)),
      Seq(row(idSchema, LongJ.valueOf(7))))
    assert(!opened)
  }

  test("failure during eager root execution cancels every launched source") {
    val first = new ScanFile(FileStatus.of("file:///table/failing.parquet", 10, 1))
    val second = new ScanFile(FileStatus.of("file:///table/blocked.parquet", 10, 1))
    val bothStarted = new CountDownLatch(2)
    val release = new CountDownLatch(1)
    val interrupted = new CountDownLatch(1)
    val failure = new IllegalStateException("first source failed")
    val handler = new FailingFrontierParquetHandler(
      first.getFileStatus.getPath,
      bothStarted,
      release,
      interrupted,
      failure)
    val aggregate = Aggregate.ungrouped(idSchema).max(new Column("id")).build()
    val plan = new Plan(Seq(
      node(new ScanParquet(Seq(first).asJava, Seq.empty[String].asJava, idSchema)),
      node(aggregate, 0),
      node(new ScanParquet(Seq(second).asJava, Seq.empty[String].asJava, idSchema)),
      node(aggregate, 2),
      node(UnionAll.UNION_ALL, 1, 3)).asJava)
    val ioExecutor = Executors.newFixedThreadPool(2)

    try {
      val thrown = intercept[IllegalStateException] {
        DefaultPlanExecutor.execute(plan, mockEngine(parquetHandler = handler), ioExecutor)
      }
      assert(thrown eq failure)
      assert(interrupted.await(5, TimeUnit.SECONDS))
      assert(!ioExecutor.isShutdown)
    } finally {
      release.countDown()
      ioExecutor.shutdownNow()
      assert(ioExecutor.awaitTermination(10, TimeUnit.SECONDS))
    }
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
      starts: ArrayBuffer[String],
      outputs: Map[String, ColumnarBatch])
      extends BaseMockParquetHandler {
    override def readParquetFiles(
        files: CloseableIterator[FileStatus],
        physicalSchema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = {
      val file = files.next()
      files.close()
      new BarrierIterator(
        Seq(new FileReadResult(outputs(file.getPath), file.getPath)),
        () => {
          starts.synchronized(starts += file.getPath)
          barrier.countDown()
          assert(barrier.await(5, TimeUnit.SECONDS), "leaf scan groups did not start together")
        })
    }
  }

  private final class BarrierIterator[T](values: Seq[T], onStart: () => Unit)
      extends CloseableIterator[T] {
    private var index = 0
    private var started = false

    override def hasNext: Boolean = {
      if (!started) {
        started = true
        onStart()
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

  private final class FailingFrontierParquetHandler(
      failingPath: String,
      bothStarted: CountDownLatch,
      release: CountDownLatch,
      interrupted: CountDownLatch,
      failure: RuntimeException)
      extends BaseMockParquetHandler {
    override def readParquetFiles(
        files: CloseableIterator[FileStatus],
        physicalSchema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = {
      val file = files.next()
      files.close()
      new CloseableIterator[FileReadResult] {
        override def hasNext: Boolean = {
          bothStarted.countDown()
          if (file.getPath == failingPath) {
            assert(bothStarted.await(5, TimeUnit.SECONDS))
            throw failure
          }
          try {
            release.await()
            false
          } catch {
            case interruptedFailure: InterruptedException =>
              interrupted.countDown()
              throw new RuntimeException(interruptedFailure)
          }
        }

        override def next(): FileReadResult = throw new NoSuchElementException()

        override def close(): Unit = {}
      }
    }
  }
}
