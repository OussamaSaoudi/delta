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

import java.io.IOException
import java.io.UncheckedIOException
import java.lang.{Long => LongJ}
import java.util.Optional
import java.util.concurrent.{CountDownLatch, Executors, ExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ColumnarBatch, FilteredColumnarBatch}
import io.delta.kernel.engine.{FileReadResult, JsonHandler, ParquetHandler}
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.plans.{FileScan, ScanFile, ScanJson, ScanParquet}
import io.delta.kernel.test.{BaseMockJsonHandler, BaseMockParquetHandler, MockEngineUtils}
import io.delta.kernel.types._
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import PlanTestUtils._
import org.scalatest.funsuite.AnyFunSuite

class FileScanExecutorSuite extends AnyFunSuite with MockEngineUtils {
  private val readSchema = new StructType().add("id", LongType.LONG, false)
  private val outputSchema = new StructType()
    .add("id", LongType.LONG, false)
    .add("part", StringType.STRING, false)
  private val constantsSchema = new StructType().add("part", StringType.STRING, false)

  private def batch(ids: Long*): ColumnarBatch =
    columnarBatch(
      readSchema,
      ids.map(id => row(readSchema, LongJ.valueOf(id))))

  private def scanFile(path: String, part: String = "a"): ScanFile =
    new ScanFile(
      FileStatus.of(path, 10, 20),
      row(constantsSchema, part))

  private def plainFile(path: String): ScanFile =
    new ScanFile(FileStatus.of(path, 10, 20))

  test("Parquet submits unique files together and preserves native batches") {
    val firstFile = plainFile("file:///table/first")
    val secondFile = plainFile("file:///table/second")
    val firstBatch = batch(1, 2)
    val secondBatch = batch(3)
    val handler = new TrackingParquetHandler(Map(
      firstFile.getFileStatus.getPath -> Seq(firstBatch),
      secondFile.getFileStatus.getPath -> Seq(secondBatch)))
    val scan = new ScanParquet(
      Seq(firstFile, secondFile).asJava,
      Seq.empty[String].asJava,
      readSchema)

    val iterator = FileScanExecutor.execute(scan, mockEngine(parquetHandler = handler))
    assert(handler.calls.map(_.files.map(_.getPath)) === Seq(Seq(
      firstFile.getFileStatus.getPath,
      secondFile.getFileStatus.getPath)))
    val result = iterator.toInMemoryList().asScala.toSeq

    assert(result.map(_.getData) === Seq(firstBatch, secondBatch))
    assert(result.forall(_.getSelectionVector.isEmpty))
    assert(handler.readers.map(_.closeCalls) === Seq(1))
  }

  test("JSON submits all files together when no file identity is needed") {
    val firstFile = plainFile("file:///table/first")
    val secondFile = plainFile("file:///table/second")
    val handler = new TrackingJsonHandler(Map(
      firstFile.getFileStatus.getPath -> Seq(batch(1)),
      secondFile.getFileStatus.getPath -> Seq(batch(2))))
    val scan = new ScanJson(
      Seq(firstFile, secondFile).asJava,
      Seq.empty[String].asJava,
      readSchema)

    val iterator = FileScanExecutor.execute(scan, mockEngine(jsonHandler = handler))
    assert(handler.calls.size === 1)
    assert(rows(iterator.toInMemoryList().asScala.toSeq).map(_.getLong(0)) === Seq(1, 2))
  }

  test("Parquet and JSON splice file constants without copying read columns") {
    val file = scanFile("file:///table/part=a/data", "a")
    val input = batch(1, 2)

    def check(scan: FileScan, result: FilteredColumnarBatch, call: ReadCall): Unit = {
      assert(call.schema === readSchema)
      assert(result.getData.getSchema === outputSchema)
      assert(result.getData.getColumnVector(0) eq input.getColumnVector(0))
      val resultRows = rows(Seq(result))
      assert(resultRows.map(_.getLong(0)) === Seq(1, 2))
      assert(resultRows.map(_.getString(1)) === Seq("a", "a"))
      assert(scan.getSchema === outputSchema)
    }

    val parquet = new TrackingParquetHandler(Map(file.getFileStatus.getPath -> Seq(input)))
    val parquetScan = new ScanParquet(
      Seq(file).asJava,
      Seq("part").asJava,
      outputSchema)
    val parquetResult = FileScanExecutor
      .execute(parquetScan, mockEngine(parquetHandler = parquet))
      .toInMemoryList()
    check(parquetScan, parquetResult.get(0), parquet.calls.head)

    val json = new TrackingJsonHandler(Map(file.getFileStatus.getPath -> Seq(input)))
    val jsonScan = new ScanJson(Seq(file).asJava, Seq("part").asJava, outputSchema)
    val jsonResult = FileScanExecutor
      .execute(jsonScan, mockEngine(jsonHandler = json))
      .toInMemoryList()
    check(jsonScan, jsonResult.get(0), json.calls.head)
  }

  test("Parquet supports repeated paths with different constants") {
    val first = scanFile("file:///table/repeated", "a")
    val second = scanFile("file:///table/repeated", "b")
    val handler = new TrackingParquetHandler(Map(first.getFileStatus.getPath -> Seq(batch(1))))
    val scan = new ScanParquet(
      Seq(first, second).asJava,
      Seq("part").asJava,
      outputSchema)

    val result = FileScanExecutor
      .execute(scan, mockEngine(parquetHandler = handler))
      .toInMemoryList()
      .asScala
      .toSeq

    assert(handler.calls.size === 2)
    assert(rows(result).map(row => row.getLong(0) -> row.getString(1)) ===
      Seq(1L -> "a", 1L -> "b"))
  }

  test("JSON row indices continue across batches and reset for each file") {
    val rowIndex = StructField.createMetadataColumn("index", MetadataColumnSpec.ROW_INDEX)
    val schema = new StructType().add("id", LongType.LONG, false).add(rowIndex)
    val first = plainFile("file:///table/first")
    val second = plainFile("file:///table/second")
    val handler = new TrackingJsonHandler(Map(
      first.getFileStatus.getPath -> Seq(batch(10, 11), batch(12)),
      second.getFileStatus.getPath -> Seq(batch(20, 21))))
    val scan = new ScanJson(
      Seq(first, second).asJava,
      Seq.empty[String].asJava,
      schema)

    val iterator = FileScanExecutor.execute(scan, mockEngine(jsonHandler = handler))
    assert(handler.calls.size === 2)
    assert(handler.calls.forall(_.schema === readSchema))
    val result = rows(iterator.toInMemoryList().asScala.toSeq)

    assert(result.map(row => row.getLong(0) -> row.getLong(1)) === Seq(
      10L -> 0L,
      11L -> 1L,
      12L -> 2L,
      20L -> 0L,
      21L -> 1L))
  }

  test("closing a per-file scan closes every eagerly opened reader") {
    val files = Seq(scanFile("file:///table/a", "a"), scanFile("file:///table/b", "b"))
    val handler = new TrackingJsonHandler(files.map(file =>
      file.getFileStatus.getPath -> Seq(batch(1))).toMap)
    val scan = new ScanJson(files.asJava, Seq("part").asJava, outputSchema)

    val iterator = FileScanExecutor.execute(scan, mockEngine(jsonHandler = handler))
    assert(handler.calls.size === 2)
    assert(handler.readers.forall(_.hasNextCalls === 0))
    iterator.close()

    assert(handler.readers.map(_.closeCalls) === Seq(1, 1))
  }

  test("parallel Parquet starts every file read together and preserves file order") {
    val first = plainFile("file:///table/first")
    val second = plainFile("file:///table/second")
    val firstRelease = new CountDownLatch(1)
    val bothStarted = new CountDownLatch(2)
    val secondFinished = new CountDownLatch(1)
    val firstReadAhead = new CountDownLatch(1)
    val neverInterrupted = new CountDownLatch(1)
    val handler = new FactoryParquetHandler(
      Map(
        first.getFileStatus.getPath -> Seq(batch(1), batch(3)),
        second.getFileStatus.getPath -> Seq(batch(2))),
      (path, values) => {
        val release =
          if (path == first.getFileStatus.getPath) firstRelease else new CountDownLatch(0)
        val finished =
          if (path == second.getFileStatus.getPath) secondFinished else new CountDownLatch(1)
        val readAhead =
          if (path == first.getFileStatus.getPath) firstReadAhead else new CountDownLatch(0)
        new BlockingIterator(
          values,
          bothStarted,
          release,
          finished,
          neverInterrupted,
          readAhead)
      })
    val scan = new ScanParquet(
      Seq(first, second).asJava,
      Seq.empty[String].asJava,
      readSchema)
    val executor = Executors.newFixedThreadPool(2)
    val iterator = FileScanExecutor.execute(
      scan,
      mockEngine(parquetHandler = handler),
      executor)

    try {
      assert(bothStarted.await(5, TimeUnit.SECONDS))
      assert(secondFinished.await(5, TimeUnit.SECONDS))
      firstRelease.countDown()

      assert(iterator.hasNext)
      val firstResult = iterator.next()
      assert(firstReadAhead.await(5, TimeUnit.SECONDS))
      val remaining = iterator.toInMemoryList().asScala.toSeq
      val result = rows(firstResult +: remaining)
      assert(result.map(_.getLong(0)) === Seq(1L, 3L, 2L))
      assert(handler.calls.map(_.files.map(_.getPath)) === Seq(
        Seq(
          first.getFileStatus.getPath),
        Seq(second.getFileStatus.getPath)))
      assert(!executor.isShutdown, "the caller owns the I/O executor")
    } finally {
      firstRelease.countDown()
      iterator.close()
      shutdown(executor)
    }
  }

  test("parallel JSON preserves row indices while priming files") {
    val rowIndex = StructField.createMetadataColumn("index", MetadataColumnSpec.ROW_INDEX)
    val schema = new StructType().add("id", LongType.LONG, false).add(rowIndex)
    val first = plainFile("file:///table/first")
    val second = plainFile("file:///table/second")
    val handler = new TrackingJsonHandler(Map(
      first.getFileStatus.getPath -> Seq(batch(10, 11), batch(12)),
      second.getFileStatus.getPath -> Seq(batch(20))))
    val scan = new ScanJson(
      Seq(first, second).asJava,
      Seq.empty[String].asJava,
      schema)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val result = FileScanExecutor
        .execute(scan, mockEngine(jsonHandler = handler), executor)
        .toInMemoryList()
        .asScala
        .toSeq
      assert(rows(result).map(row => row.getLong(0) -> row.getLong(1)) === Seq(
        10L -> 0L,
        11L -> 1L,
        12L -> 2L,
        20L -> 0L))
      assert(handler.calls.size === 2)
    } finally {
      shutdown(executor)
    }
  }

  test("parallel read failure closes and cancels every file reader") {
    val first = plainFile("file:///table/failing")
    val second = plainFile("file:///table/blocked")
    val bothStarted = new CountDownLatch(2)
    val block = new CountDownLatch(1)
    val interrupted = new CountDownLatch(1)
    val failure = new IllegalStateException("first read failed")
    val handler = new FactoryParquetHandler(
      Map(
        first.getFileStatus.getPath -> Seq(batch(1)),
        second.getFileStatus.getPath -> Seq(batch(2))),
      (path, values) => {
        if (path == first.getFileStatus.getPath) {
          new FailingIterator(values, bothStarted, failure)
        } else {
          new BlockingIterator(
            values,
            bothStarted,
            block,
            new CountDownLatch(1),
            interrupted)
        }
      })
    val scan = new ScanParquet(
      Seq(first, second).asJava,
      Seq.empty[String].asJava,
      readSchema)
    val executor = Executors.newFixedThreadPool(2)
    val iterator = FileScanExecutor.execute(
      scan,
      mockEngine(parquetHandler = handler),
      executor)

    try {
      assert(bothStarted.await(5, TimeUnit.SECONDS))
      val thrown = intercept[IllegalStateException](iterator.hasNext)
      assert(thrown eq failure)
      assert(interrupted.await(5, TimeUnit.SECONDS))
      assert(handler.readers.map(_.closeCalls) === Seq(1, 1))
      assert(!executor.isShutdown)
    } finally {
      block.countDown()
      iterator.close()
      shutdown(executor)
    }
  }

  test("closing a parallel scan cancels in-flight reads without owning the executor") {
    val files = Seq(plainFile("file:///table/a"), plainFile("file:///table/b"))
    val bothStarted = new CountDownLatch(2)
    val block = new CountDownLatch(1)
    val interrupted = new CountDownLatch(2)
    val handler = new FactoryParquetHandler(
      files.map(file => file.getFileStatus.getPath -> Seq(batch(1))).toMap,
      (_, values) =>
        new BlockingIterator(
          values,
          bothStarted,
          block,
          new CountDownLatch(1),
          interrupted))
    val scan = new ScanParquet(
      files.asJava,
      Seq.empty[String].asJava,
      readSchema)
    val executor = Executors.newFixedThreadPool(2)
    val iterator = FileScanExecutor.execute(
      scan,
      mockEngine(parquetHandler = handler),
      executor)

    try {
      assert(bothStarted.await(5, TimeUnit.SECONDS))
      iterator.close()
      assert(interrupted.await(5, TimeUnit.SECONDS))
      assert(handler.readers.map(_.closeCalls) === Seq(1, 1))
      assert(!executor.isShutdown)
    } finally {
      block.countDown()
      iterator.close()
      shutdown(executor)
    }
  }

  test("closing a parallel scan closes its reader before awaiting a blocked read") {
    val file = plainFile("file:///table/blocked")
    val started = new CountDownLatch(1)
    val handler = new FactoryParquetHandler(
      Map(file.getFileStatus.getPath -> Seq(batch(1))),
      (_, values) => new CloseUnblocksIterator(values, started))
    val scan = new ScanParquet(
      Seq(file).asJava,
      Seq.empty[String].asJava,
      readSchema)
    val ioExecutor = Executors.newSingleThreadExecutor()
    val closeExecutor = Executors.newSingleThreadExecutor()
    val iterator = FileScanExecutor.execute(
      scan,
      mockEngine(parquetHandler = handler),
      ioExecutor)

    try {
      assert(started.await(5, TimeUnit.SECONDS))
      val close = closeExecutor.submit(new Runnable {
        override def run(): Unit = iterator.close()
      })
      close.get(5, TimeUnit.SECONDS)
      assert(handler.readers.map(_.closeCalls) === Seq(1))
    } finally {
      iterator.close()
      shutdown(closeExecutor)
      shutdown(ioExecutor)
    }
  }

  test("parallel scan opens every file reader concurrently") {
    val first = plainFile("file:///table/first")
    val second = plainFile("file:///table/second")
    val bothOpening = new CountDownLatch(2)
    val releaseOpen = new CountDownLatch(1)
    val handler = new FactoryParquetHandler(
      Map(
        first.getFileStatus.getPath -> Seq(batch(1)),
        second.getFileStatus.getPath -> Seq(batch(2))),
      (_, values) => {
        bothOpening.countDown()
        assert(releaseOpen.await(5, TimeUnit.SECONDS))
        new TrackingIterator(values)
      })
    val scan = new ScanParquet(
      Seq(first, second).asJava,
      Seq.empty[String].asJava,
      readSchema)
    val executor = Executors.newFixedThreadPool(2)
    val iterator = FileScanExecutor.execute(
      scan,
      mockEngine(parquetHandler = handler),
      executor)

    try {
      assert(bothOpening.await(5, TimeUnit.SECONDS))
      releaseOpen.countDown()
      val result = rows(iterator.toInMemoryList().asScala.toSeq)
      assert(result.map(_.getLong(0)) === Seq(1L, 2L))
    } finally {
      releaseOpen.countDown()
      iterator.close()
      shutdown(executor)
    }
  }

  test("failure opening a later reader closes readers already opened") {
    val files = Seq(scanFile("file:///table/a", "a"), scanFile("file:///table/b", "b"))
    val handler = new TrackingJsonHandler(
      files.map(file => file.getFileStatus.getPath -> Seq(batch(1))).toMap,
      failOnCall = 2)
    val scan = new ScanJson(files.asJava, Seq("part").asJava, outputSchema)

    assertThrows[UncheckedIOException] {
      FileScanExecutor.execute(scan, mockEngine(jsonHandler = handler))
    }
    assert(handler.readers.map(_.closeCalls) === Seq(1))
  }

  private case class ReadCall(files: Seq[FileStatus], schema: StructType)

  private def shutdown(executor: ExecutorService): Unit = {
    executor.shutdownNow()
    assert(executor.awaitTermination(5, TimeUnit.SECONDS))
  }

  private class TrackingJsonHandler(
      outputs: Map[String, Seq[ColumnarBatch]],
      failOnCall: Int = -1)
      extends BaseMockJsonHandler {
    val calls = ArrayBuffer.empty[ReadCall]
    val readers = ArrayBuffer.empty[TrackingIterator[ColumnarBatch]]

    override def readJsonFiles(
        fileIter: CloseableIterator[FileStatus],
        physicalSchema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
      val files = fileIter.toInMemoryList().asScala.toSeq
      val callNumber = this.synchronized {
        calls += ReadCall(files, physicalSchema)
        calls.size
      }
      if (callNumber == failOnCall) {
        throw new IOException("open failed")
      }
      val reader = new TrackingIterator(files.flatMap(file => outputs(file.getPath)))
      this.synchronized {
        readers += reader
      }
      reader
    }
  }

  private class TrackingParquetHandler(outputs: Map[String, Seq[ColumnarBatch]])
      extends BaseMockParquetHandler {
    val calls = ArrayBuffer.empty[ReadCall]
    val readers = ArrayBuffer.empty[TrackingIterator[FileReadResult]]

    override def readParquetFiles(
        fileIter: CloseableIterator[FileStatus],
        physicalSchema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = {
      val files = fileIter.toInMemoryList().asScala.toSeq
      calls += ReadCall(files, physicalSchema)
      val results = files.flatMap(file =>
        outputs(file.getPath).map(batch => new FileReadResult(batch, file.getPath)))
      val reader = new TrackingIterator(results)
      this.synchronized {
        readers += reader
      }
      reader
    }
  }

  private class FactoryParquetHandler(
      outputs: Map[String, Seq[ColumnarBatch]],
      factory: (String, Seq[FileReadResult]) => TrackingIterator[FileReadResult])
      extends BaseMockParquetHandler {
    val calls = ArrayBuffer.empty[ReadCall]
    val readers = ArrayBuffer.empty[TrackingIterator[FileReadResult]]

    override def readParquetFiles(
        fileIter: CloseableIterator[FileStatus],
        physicalSchema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = {
      val files = fileIter.toInMemoryList().asScala.toSeq
      this.synchronized {
        calls += ReadCall(files, physicalSchema)
      }
      assert(files.size === 1)
      val path = files.head.getPath
      val reader = factory(
        path,
        outputs(path).map(batch => new FileReadResult(batch, path)))
      this.synchronized {
        readers += reader
      }
      reader
    }
  }

  private class BlockingIterator[T](
      values: Seq[T],
      started: CountDownLatch,
      release: CountDownLatch,
      finished: CountDownLatch,
      interrupted: CountDownLatch,
      readAheadStarted: CountDownLatch = new CountDownLatch(0))
      extends TrackingIterator[T](values) {
    private val first = new AtomicBoolean(true)

    override def next(): T = {
      if (first.compareAndSet(true, false)) {
        started.countDown()
        try {
          release.await()
          super.next()
        } catch {
          case failure: InterruptedException =>
            interrupted.countDown()
            Thread.currentThread().interrupt()
            throw new RuntimeException("file read interrupted", failure)
        } finally {
          finished.countDown()
        }
      } else {
        readAheadStarted.countDown()
        super.next()
      }
    }
  }

  private class FailingIterator[T](
      values: Seq[T],
      started: CountDownLatch,
      failure: RuntimeException)
      extends TrackingIterator[T](values) {
    private val first = new AtomicBoolean(true)

    override def next(): T = {
      if (first.compareAndSet(true, false)) {
        started.countDown()
        throw failure
      }
      super.next()
    }
  }

  private class CloseUnblocksIterator[T](values: Seq[T], started: CountDownLatch)
      extends TrackingIterator[T](values) {
    private val closed = new CountDownLatch(1)

    override def next(): T = {
      started.countDown()
      var waiting = true
      while (waiting) {
        try {
          closed.await()
          waiting = false
        } catch {
          case _: InterruptedException => // Closing the reader is the only unblock mechanism.
        }
      }
      super.next()
    }

    override def close(): Unit = {
      closed.countDown()
      super.close()
    }
  }
}
