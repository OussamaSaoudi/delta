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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ColumnarBatch, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch
import io.delta.kernel.engine.{FileReadResult, JsonHandler, ParquetHandler}
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.plans.{FileScan, ScanFile, ScanJson, ScanParquet}
import io.delta.kernel.test.{BaseMockJsonHandler, BaseMockParquetHandler, MockEngineUtils}
import io.delta.kernel.types._
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.scalatest.funsuite.AnyFunSuite

class FileScanExecutorSuite extends AnyFunSuite with MockEngineUtils {
  private val readSchema = new StructType().add("id", LongType.LONG, false)
  private val outputSchema = new StructType()
    .add("id", LongType.LONG, false)
    .add("part", StringType.STRING, false)
  private val constantsSchema = new StructType().add("part", StringType.STRING, false)

  private def batch(ids: Long*): ColumnarBatch =
    new DefaultRowBasedColumnarBatch(
      readSchema,
      ids.map[Row](id =>
        GenericRow.fromValues(readSchema, Seq(LongJ.valueOf(id)).asJava)).asJava)

  private def scanFile(path: String, part: String = "a"): ScanFile =
    new ScanFile(
      FileStatus.of(path, 10, 20),
      GenericRow.fromValues(constantsSchema, Seq(part).asJava))

  private def plainFile(path: String): ScanFile =
    new ScanFile(FileStatus.of(path, 10, 20))

  private def rows(batches: Seq[FilteredColumnarBatch]): Seq[Row] =
    batches.flatMap { batch =>
      val iterator = batch.getRows
      try iterator.asScala.toSeq
      finally iterator.close()
    }

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
      calls += ReadCall(files, physicalSchema)
      if (calls.size == failOnCall) {
        throw new IOException("open failed")
      }
      val reader = new TrackingIterator(files.flatMap(file => outputs(file.getPath)))
      readers += reader
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
      readers += reader
      reader
    }
  }

  private class TrackingIterator[T](values: Seq[T]) extends CloseableIterator[T] {
    private var index = 0
    var hasNextCalls = 0
    var closeCalls = 0

    override def hasNext: Boolean = {
      hasNextCalls += 1
      index < values.size
    }

    override def next(): T = {
      if (index >= values.size) {
        throw new NoSuchElementException
      }
      val value = values(index)
      index += 1
      value
    }

    override def close(): Unit = closeCalls += 1
  }
}
