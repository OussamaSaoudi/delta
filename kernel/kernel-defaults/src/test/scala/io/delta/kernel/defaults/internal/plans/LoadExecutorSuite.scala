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

import io.delta.kernel.data.{ColumnarBatch, Row}
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch
import io.delta.kernel.engine.FileReadResult
import io.delta.kernel.expressions.{Column, Predicate}
import io.delta.kernel.internal.actions.DeletionVectorDescriptor
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.plans.{FileType, Load, LoadColumnFileMeta, PlanBuilder}
import io.delta.kernel.test.{BaseMockFileSystemClient, BaseMockJsonHandler, BaseMockParquetHandler}
import io.delta.kernel.types._
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.scalatest.funsuite.AnyFunSuite

class LoadExecutorSuite extends AnyFunSuite with PlanExecutionSuiteBase {
  private val metadataSchema = new StructType()
    .add("path", StringType.STRING, false)
    .add("size", LongType.LONG, true)
    .add("num_records", LongType.LONG, true)
    .add("part", StringType.STRING, true)
    .add("dv", DeletionVectorDescriptor.READ_SCHEMA, true)

  private def row(schema: StructType, values: AnyRef*): Row =
    GenericRow.fromValues(schema, values.asJava)

  private def metadataRow(
      path: String,
      size: java.lang.Long,
      part: String,
      deletionVector: Row = null): Row =
    row(metadataSchema, path, size, null, part, deletionVector)

  private def fileMeta(
      path: Column = new Column("path"),
      size: Column = new Column("size"),
      records: Column = new Column("num_records")): LoadColumnFileMeta =
    new LoadColumnFileMeta(path, size, records)

  private def load(
      outputSchema: StructType,
      fileType: FileType,
      baseUri: Optional[URI],
      constants: Seq[String],
      meta: LoadColumnFileMeta = fileMeta(),
      deletionVector: Column = new Column("dv")): Load =
    new Load(
      outputSchema,
      fileType,
      baseUri,
      constants.asJava,
      meta,
      deletionVector)

  test("Parquet Load materializes file descriptors and delegates to shared scan") {
    val outputSchema = new StructType()
      .add("id", LongType.LONG, false)
      .add("part", StringType.STRING, true)
    val relative = "file:/table/first.parquet"
    val absolute = "file:///other/second.parquet"
    val rows = Seq(
      metadataRow("first.parquet", null, "a"),
      metadataRow(absolute, LongJ.valueOf(30), "b"))
    val handler = new ParquetRows(Map(relative -> 1L, absolute -> 2L))
    val statusPaths = ArrayBuffer.empty[String]
    val fileSystem = new BaseMockFileSystemClient {
      override def getFileStatus(path: String): FileStatus = {
        statusPaths += path
        FileStatus.of(path, 20, 10)
      }
    }
    val operator = load(
      outputSchema,
      FileType.PARQUET,
      Optional.of(URI.create("file:///table/")),
      Seq("part"))

    checkRows(
      PlanBuilder.values(metadataSchema, rows.asJava).load(operator),
      mockEngine(fileSystemClient = fileSystem, parquetHandler = handler),
      Seq(
        row(outputSchema, LongJ.valueOf(1), "a"),
        row(outputSchema, LongJ.valueOf(2), "b")))

    assert(statusPaths === Seq(relative))
    assert(handler.files.map(_.getPath) === Seq(relative, absolute))
    assert(handler.files.map(_.getSize) === Seq(20L, 30L))
  }

  test("JSON Load resolves nested metadata and preserves row index and constants") {
    val rowIndex = StructField.createMetadataColumn("row_index", MetadataColumnSpec.ROW_INDEX)
    val outputSchema = new StructType()
      .add("id", LongType.LONG, false)
      .add(rowIndex)
      .add("version", LongType.LONG, false)
    val nestedMetadata = new StructType()
      .add("path", StringType.STRING, false)
      .add("size", LongType.LONG, true)
      .add("num_records", LongType.LONG, true)
      .add("dv", DeletionVectorDescriptor.READ_SCHEMA, true)
    val inputSchema = new StructType()
      .add("meta", nestedMetadata, false)
      .add("version", LongType.LONG, false)
    val metadata = row(
      nestedMetadata,
      "file:///table/actions.json",
      LongJ.valueOf(10),
      null,
      null)
    val input = row(inputSchema, metadata, LongJ.valueOf(7))
    val operator = load(
      outputSchema,
      FileType.JSON,
      Optional.empty(),
      Seq("version"),
      fileMeta(
        new Column(Array("meta", "path")),
        new Column(Array("meta", "size")),
        new Column(Array("meta", "num_records"))),
      new Column(Array("meta", "dv")))
    val handler = new JsonRows(Map("file:///table/actions.json" -> 9L))

    checkRows(
      PlanBuilder.values(inputSchema, Seq(input).asJava).load(operator),
      mockEngine(jsonHandler = handler),
      Seq(row(outputSchema, LongJ.valueOf(9), LongJ.valueOf(0), LongJ.valueOf(7))))
  }

  test("Load forwards deletion vectors through the shared scan path") {
    val outputSchema = new StructType().add("id", LongType.LONG, false)
    val deletionVector = new DeletionVectorDescriptor(
      DeletionVectorDescriptor.INLINE_DV_MARKER,
      "",
      Optional.empty(),
      0,
      0)
    val input = metadataRow(
      "file:///table/data.parquet",
      LongJ.valueOf(10),
      null,
      deletionVector.toRow)
    val operator = load(
      outputSchema,
      FileType.PARQUET,
      Optional.of(URI.create("file:///table/")),
      Seq.empty)
    val handler = new ParquetRows(Map("file:///table/data.parquet" -> 5L))

    checkRows(
      PlanBuilder.values(metadataSchema, Seq(input).asJava).load(operator),
      mockEngine(parquetHandler = handler),
      Seq(row(outputSchema, LongJ.valueOf(5))))

    assert(handler.schemas.head.indexOf(MetadataColumnSpec.ROW_INDEX) >= 0)
  }

  test("Load rejects negative known file sizes before opening a reader") {
    val outputSchema = new StructType().add("id", LongType.LONG, false)
    val operator = load(
      outputSchema,
      FileType.PARQUET,
      Optional.of(URI.create("file:///table/")),
      Seq.empty)
    val plan = PlanBuilder.values(
      metadataSchema,
      Seq(metadataRow("data.parquet", LongJ.valueOf(-1), null)).asJava).load(operator)

    val error = intercept[IllegalArgumentException] {
      DefaultPlanExecutor.execute(plan.build(), mockEngine())
    }
    assert(error.getMessage.contains("non-negative"))
  }

  private def dataBatch(schema: StructType, id: Long): ColumnarBatch = {
    val values = schema.fields.asScala.map { field =>
      if (field.getMetadataColumnSpec == MetadataColumnSpec.ROW_INDEX) LongJ.valueOf(0)
      else LongJ.valueOf(id)
    }
    new DefaultRowBasedColumnarBatch(schema, Seq(row(schema, values: _*)).asJava)
  }

  private class ParquetRows(ids: Map[String, Long]) extends BaseMockParquetHandler {
    val files = ArrayBuffer.empty[FileStatus]
    val schemas = ArrayBuffer.empty[StructType]

    override def readParquetFiles(
        input: CloseableIterator[FileStatus],
        schema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = {
      val file = input.next()
      files += file
      schemas += schema
      closeable(Seq(new FileReadResult(dataBatch(schema, ids(file.getPath)), file.getPath)))
    }
  }

  private class JsonRows(ids: Map[String, Long]) extends BaseMockJsonHandler {
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
