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

import io.delta.kernel.data.{ColumnarBatch, Row}
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch
import io.delta.kernel.engine.FileReadResult
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.plans.{PlanBuilder, ScanFile}
import io.delta.kernel.test.BaseMockParquetHandler
import io.delta.kernel.types._
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.scalatest.funsuite.AnyFunSuite

class FileScanExecutorSuite extends AnyFunSuite with PlanExecutionSuiteBase {
  private val readSchema = new StructType().add("id", LongType.LONG, false)
  private val outputSchema = readSchema.add("part", StringType.STRING, false)
  private val constantsSchema = new StructType().add("part", StringType.STRING, false)

  private def row(schema: StructType, values: AnyRef*): Row =
    GenericRow.fromValues(schema, values.asJava)

  private def batch(ids: Long*): ColumnarBatch =
    new DefaultRowBasedColumnarBatch(
      readSchema,
      ids.map(id => row(readSchema, LongJ.valueOf(id))).asJava)

  private def file(path: String, part: String): ScanFile =
    new ScanFile(FileStatus.of(path), row(constantsSchema, part))

  test("Parquet scans files in order and broadcasts file constants") {
    val files = Seq(file("file:///table/a", "a"), file("file:///table/b", "b"))
    val handler = new ParquetReads(Map(
      files(0).getFileStatus.getPath -> Seq(batch(1, 2)),
      files(1).getFileStatus.getPath -> Seq(batch(3))))
    val plan = PlanBuilder.scanParquet(files.asJava, Seq("part").asJava, outputSchema)

    checkRows(
      plan,
      mockEngine(parquetHandler = handler),
      Seq(row(outputSchema, LongJ.valueOf(1), "a"),
        row(outputSchema, LongJ.valueOf(2), "a"),
        row(outputSchema, LongJ.valueOf(3), "b")))

    assert(handler.files === files.map(_.getFileStatus.getPath))
    assert(handler.schemas.forall(_ === readSchema))
  }

  test("Parquet scan reuses columns returned by Kernel Java") {
    val scanFile = file("file:///table/a", "a")
    val input = batch(1)
    val handler = new ParquetReads(Map(scanFile.getFileStatus.getPath -> Seq(input)))
    val iterator = DefaultPlanExecutor.execute(
      PlanBuilder.scanParquet(Seq(scanFile).asJava, Seq("part").asJava, outputSchema).build(),
      mockEngine(parquetHandler = handler))

    val output = iterator.next().getData
    iterator.close()
    assert(output.getColumnVector(0) eq input.getColumnVector(0))
  }

  private class ParquetReads(outputs: Map[String, Seq[ColumnarBatch]])
      extends BaseMockParquetHandler {
    val files = ArrayBuffer.empty[String]
    val schemas = ArrayBuffer.empty[StructType]

    override def readParquetFiles(
        input: CloseableIterator[FileStatus],
        schema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = {
      val file = input.next()
      files += file.getPath
      schemas += schema
      closeable(outputs(file.getPath).map(new FileReadResult(_, file.getPath)))
    }
  }

  private def closeable[T](values: Seq[T]): CloseableIterator[T] =
    io.delta.kernel.internal.util.Utils.toCloseableIterator(values.iterator.asJava)
}
