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
import java.net.URI
import java.util.Optional

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ColumnarBatch, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch
import io.delta.kernel.engine.FileReadResult
import io.delta.kernel.execution.{PlanExecutor, PlanResultCache}
import io.delta.kernel.expressions.{Column, Predicate}
import io.delta.kernel.internal.actions.DeletionVectorDescriptor
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.plans._
import io.delta.kernel.test.{BaseMockFileSystemClient, BaseMockJsonHandler, BaseMockParquetHandler}
import io.delta.kernel.types._
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.scalatest.funsuite.AnyFunSuite

class DynamicScanExecutorSuite extends AnyFunSuite with PlanExecutionSuiteBase {
  private val metadataSchema = new StructType()
    .add("path", StringType.STRING, false)
    .add("size", LongType.LONG, false)
    .add("modified", LongType.LONG, false)
    .add("part", StringType.STRING)
    .add("dv", DeletionVectorDescriptor.READ_SCHEMA)
  private val fileSystem = new BaseMockFileSystemClient {}

  private def row(schema: StructType, values: AnyRef*): Row =
    GenericRow.fromValues(schema, values.asJava)

  private def metadataRow(
      path: String,
      size: Long,
      modified: Long,
      part: String,
      deletionVector: Row = null): Row =
    row(
      metadataSchema,
      path,
      LongJ.valueOf(size),
      LongJ.valueOf(modified),
      part,
      deletionVector)

  private def scan(
      input: PlanNode,
      outputSchema: StructType,
      fileType: FileType,
      constants: Seq[String],
      root: URI = URI.create("file:///table/"),
      path: Column = new Column("path"),
      size: Column = new Column("size"),
      modified: Column = new Column("modified"),
      deletionVector: Column = new Column("dv")): DynamicScan =
    new DynamicScan(
      input,
      outputSchema,
      fileType,
      root,
      constants.asJava,
      path,
      size,
      modified,
      deletionVector)

  test("Parquet DynamicScan materializes complete files and delegates to static scan") {
    val outputSchema = new StructType()
      .add("id", LongType.LONG, false)
      .add("part", StringType.STRING)
    val rows = Seq(
      metadataRow("first.parquet", 20, 10, "a"),
      metadataRow("file:///other/second.parquet", 30, 11, "b"))
    val input = new Values(metadataSchema, rows.asJava)
    val handler = new ParquetRows(Map(
      "file:/table/first.parquet" -> 1L,
      "file:///other/second.parquet" -> 2L))

    checkRows(
      scan(input, outputSchema, FileType.PARQUET, Seq("part")),
      mockEngine(parquetHandler = handler),
      Seq(row(outputSchema, LongJ.valueOf(1), "a"), row(outputSchema, LongJ.valueOf(2), "b")))

    assert(handler.files.map(_.getSize) === Seq(20L, 30L))
    assert(handler.files.map(_.getModificationTime) === Seq(10L, 11L))
  }

  test("JSON DynamicScan resolves nested metadata and preserves row index and constants") {
    val rowIndex = StructField.createMetadataColumn("row_index", MetadataColumnSpec.ROW_INDEX)
    val outputSchema = new StructType()
      .add("id", LongType.LONG, false)
      .add(rowIndex)
      .add("version", LongType.LONG, false)
    val nested = new StructType()
      .add("path", StringType.STRING, false)
      .add("size", LongType.LONG, false)
      .add("modified", LongType.LONG, false)
      .add("dv", DeletionVectorDescriptor.READ_SCHEMA)
    val inputSchema = new StructType()
      .add("meta", nested, false)
      .add("version", LongType.LONG, false)
    val metadata = row(
      nested,
      "file:///table/actions.json",
      LongJ.valueOf(10),
      LongJ.valueOf(5),
      null)
    val input = new Values(inputSchema, Seq(row(inputSchema, metadata, LongJ.valueOf(7))).asJava)
    val dynamic = scan(
      input,
      outputSchema,
      FileType.JSON,
      Seq("version"),
      path = new Column(Array("meta", "path")),
      size = new Column(Array("meta", "size")),
      modified = new Column(Array("meta", "modified")),
      deletionVector = new Column(Array("meta", "dv")))
    val handler = new JsonRows(Map("file:///table/actions.json" -> 9L))

    checkRows(
      dynamic,
      mockEngine(jsonHandler = handler),
      Seq(row(outputSchema, LongJ.valueOf(9), LongJ.valueOf(0), LongJ.valueOf(7))))
  }

  test("DynamicScan rejects non-positive file sizes before opening the reader") {
    val outputSchema = new StructType().add("id", LongType.LONG, false)
    val input = new Values(
      metadataSchema,
      Seq(metadataRow("data.parquet", 0, 10, null)).asJava)
    val dynamic = scan(input, outputSchema, FileType.PARQUET, Seq.empty)
    val cache = new PlanResultCache(1, Long.MaxValue)
    val batches = new PlanExecutor(mockEngine(), cache).execute(dynamic)

    val error = intercept[IllegalArgumentException](batches.hasNext)
    assert(error.getMessage.contains("positive"))
    batches.close()
    cache.close()
  }

  private def dataBatch(schema: StructType, id: Long): ColumnarBatch = {
    val values = schema.fields.asScala.map[AnyRef] { field =>
      if (field.getMetadataColumnSpec == MetadataColumnSpec.ROW_INDEX) LongJ.valueOf(0)
      else LongJ.valueOf(id)
    }.toSeq
    new DefaultRowBasedColumnarBatch(schema, Seq(row(schema, values: _*)).asJava)
  }

  private class ParquetRows(ids: Map[String, Long]) extends BaseMockParquetHandler {
    val files = ArrayBuffer.empty[FileStatus]

    override def readParquetFiles(scan: ScanParquet): CloseableIterator[FilteredColumnarBatch] =
      FileScanExecutor.execute(scan, this, fileSystem)

    override def readParquetFiles(
        input: CloseableIterator[FileStatus],
        schema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = {
      val statuses = input.toInMemoryList.asScala.toSeq
      files ++= statuses
      closeable(statuses.map { file =>
        new FileReadResult(dataBatch(schema, ids(file.getPath)), file.getPath)
      })
    }
  }

  private class JsonRows(ids: Map[String, Long]) extends BaseMockJsonHandler {
    override def readJsonFiles(scan: ScanJson): CloseableIterator[FilteredColumnarBatch] =
      FileScanExecutor.execute(scan, this, fileSystem)

    override def readJsonFiles(
        input: CloseableIterator[FileStatus],
        schema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
      val file = input.next()
      closeable(Seq(dataBatch(schema, ids(file.getPath))))
    }
  }

  private def closeable[T](values: Seq[T]): CloseableIterator[T] =
    io.delta.kernel.internal.util.Utils.toCloseableIterator(values.iterator.asJava)
}
