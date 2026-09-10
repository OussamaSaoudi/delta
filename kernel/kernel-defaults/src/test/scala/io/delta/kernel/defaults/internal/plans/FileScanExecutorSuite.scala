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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ColumnarBatch, FilteredColumnarBatch, Row}
import io.delta.kernel.data.FilteredColumnarBatch.Lifetime
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch
import io.delta.kernel.engine.FileReadResult
import io.delta.kernel.execution.{PlanExecutor, PlanResultCache}
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.actions.DeletionVectorDescriptor
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.plans.{ScanFile, ScanJson, ScanParquet}
import io.delta.kernel.test.{BaseMockFileSystemClient, BaseMockJsonHandler, BaseMockParquetHandler}
import io.delta.kernel.types._
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.scalatest.funsuite.AnyFunSuite

class FileScanExecutorSuite extends AnyFunSuite with PlanExecutionSuiteBase {
  private val readSchema = new StructType().add("id", LongType.LONG, false)
  private val outputSchema = readSchema.add("part", StringType.STRING, false)
  private val constantsSchema = new StructType().add("part", StringType.STRING, false)
  private val fileSystem = new BaseMockFileSystemClient {}

  private def row(schema: StructType, values: AnyRef*): Row =
    GenericRow.fromValues(schema, values.asJava)

  private def batch(ids: Long*): ColumnarBatch =
    new DefaultRowBasedColumnarBatch(
      readSchema,
      ids.map(id => row(readSchema, LongJ.valueOf(id))).asJava)

  private def file(path: String, part: String): ScanFile =
    new ScanFile(
      FileStatus.of(path, 10, 20),
      row(constantsSchema, part),
      Optional.empty())

  test("Parquet scans files together and broadcasts constants without copying data") {
    val files = Seq(file("file:///table/a", "a"), file("file:///table/b", "b"))
    val firstBatch = batch(1, 2)
    val handler = new ParquetReads(Map(
      files(0).getFileStatus.getPath -> Seq(firstBatch),
      files(1).getFileStatus.getPath -> Seq(batch(3))))
    val plan = new ScanParquet(
      files.asJava,
      Optional.empty(),
      Seq("part").asJava,
      outputSchema,
      Optional.empty())

    checkRows(
      plan,
      mockEngine(parquetHandler = handler),
      Seq(
        row(outputSchema, LongJ.valueOf(1), "a"),
        row(outputSchema, LongJ.valueOf(2), "a"),
        row(outputSchema, LongJ.valueOf(3), "b")))

    val cache = new PlanResultCache(1, Long.MaxValue)
    val batches = new PlanExecutor(mockEngine(parquetHandler = handler), cache).execute(plan)
    val first = batches.next()
    assert(first.getData.getColumnVector(0) eq firstBatch.getColumnVector(0))
    assert(first.getLifetime === Lifetime.OWNED)
    batches.close()
    cache.close()
    assert(handler.schemas.forall(_ === readSchema))
  }

  test("scan applies deletion vectors through an internal row-index column") {
    val path = "file:///table/with-dv"
    val deletionVector = new DeletionVectorDescriptor(
      DeletionVectorDescriptor.INLINE_DV_MARKER,
      "",
      Optional.empty(),
      0,
      0)
    val scanFile = new ScanFile(
      FileStatus.of(path, 10, 20),
      row(constantsSchema, "a"),
      Optional.of(deletionVector))
    val privateSchema = readSchema.add(
      StructField.createMetadataColumn(
        "__delta_kernel_scan_row_index",
        MetadataColumnSpec.ROW_INDEX))
    val input = new DefaultRowBasedColumnarBatch(
      privateSchema,
      Seq(row(privateSchema, LongJ.valueOf(1), LongJ.valueOf(0))).asJava)
    val handler = new ParquetReads(Map(path -> Seq(input)))
    val scan = new ScanParquet(
      Seq(scanFile).asJava,
      Optional.empty(),
      Seq("part").asJava,
      outputSchema,
      Optional.empty())
    val cache = new PlanResultCache(1, Long.MaxValue)
    val batches = new PlanExecutor(mockEngine(parquetHandler = handler), cache).execute(scan)

    val result = batches.next()
    assert(result.getData.getSchema === outputSchema)
    assert(result.getSelectionVector.isPresent)
    assert(result.isSelected(0))
    assert(result.getLifetime === Lifetime.OWNED)
    batches.close()
    cache.close()
  }

  test("JSON scans reset physical row indices for each file") {
    val rowIndex = StructField.createMetadataColumn("index", MetadataColumnSpec.ROW_INDEX)
    val schema = readSchema.add(rowIndex)
    val emptyConstants = GenericRow.fromOwnedValues(new StructType(), Array.empty[AnyRef])
    val files = Seq("a", "b").map { name =>
      new ScanFile(
        FileStatus.of(s"file:///table/$name", 10, 20),
        emptyConstants,
        Optional.empty())
    }
    val handler = new JsonReads(Map(
      files(0).getFileStatus.getPath -> Seq(batch(10, 11), batch(12)),
      files(1).getFileStatus.getPath -> Seq(batch(20))))
    val scan = new ScanJson(files.asJava, Optional.empty(), Seq.empty[String].asJava, schema)

    checkRows(
      scan,
      mockEngine(jsonHandler = handler),
      Seq(
        row(schema, LongJ.valueOf(10), LongJ.valueOf(0)),
        row(schema, LongJ.valueOf(11), LongJ.valueOf(1)),
        row(schema, LongJ.valueOf(12), LongJ.valueOf(2)),
        row(schema, LongJ.valueOf(20), LongJ.valueOf(0))))
  }

  private class ParquetReads(outputs: Map[String, Seq[ColumnarBatch]])
      extends BaseMockParquetHandler {
    val schemas = ArrayBuffer.empty[StructType]

    override def readParquetFiles(scan: ScanParquet): CloseableIterator[FilteredColumnarBatch] =
      FileScanExecutor.execute(scan, this, fileSystem)

    override def readParquetFiles(
        input: CloseableIterator[FileStatus],
        schema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = {
      val files = input.toInMemoryList.asScala.toSeq
      schemas ++= Seq.fill(files.size)(schema)
      closeable(files.flatMap { file =>
        outputs(file.getPath).map(new FileReadResult(_, file.getPath))
      })
    }
  }

  private class JsonReads(outputs: Map[String, Seq[ColumnarBatch]]) extends BaseMockJsonHandler {
    override def readJsonFiles(scan: ScanJson): CloseableIterator[FilteredColumnarBatch] =
      FileScanExecutor.execute(scan, this, fileSystem)

    override def readJsonFiles(
        input: CloseableIterator[FileStatus],
        schema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
      val file = input.next()
      closeable(outputs(file.getPath))
    }
  }

  private def closeable[T](values: Seq[T]): CloseableIterator[T] =
    io.delta.kernel.internal.util.Utils.toCloseableIterator(values.iterator.asJava)
}
