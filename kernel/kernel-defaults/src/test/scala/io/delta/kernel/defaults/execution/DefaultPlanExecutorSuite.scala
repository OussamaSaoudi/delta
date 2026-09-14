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

package io.delta.kernel.defaults.execution

import java.nio.file.{Files, Path}
import java.util.{Collections, HashMap, Optional}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.data.{ColumnarBatch, Row}
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.execution.{PlanExecutor, PlanResultCache}
import io.delta.kernel.expressions.{Column, Expression, Predicate}
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.plans.{Agg, PlanNode, ScanFile}
import io.delta.kernel.plans.PlanNode.{Aggregate, Cte, FileScan, Filter, SemiJoin, UnionAll}
import io.delta.kernel.types.{DataType, LongType, StringType, StructType}
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

class DefaultPlanExecutorSuite extends AnyFunSuite with TestUtils {
  private val checkpointSchema = new StructType()
    .add("path", StringType.STRING, false)
    .add("dv", StringType.STRING, true)
    .add("live", StringType.STRING, true)
  private val jsonSchema = checkpointSchema.add("action", StringType.STRING, false)
  private val changesSchema = jsonSchema.add("version", LongType.LONG, false)
  private val versionSchema = new StructType().add("version", LongType.LONG, false)
  private val emptySchema = new StructType()

  test("default Engine executes checkpoint and JSON metadata replay end to end") {
    withTempDir { tempDir =>
      val checkpoint = tempDir.toPath.resolve("checkpoint.parquet")
      val commit10 = tempDir.toPath.resolve("00000000000000000010.json")
      val commit11 = tempDir.toPath.resolve("00000000000000000011.json")

      writeParquet(
        checkpoint,
        batch(
          checkpointSchema,
          row(checkpointSchema, "a", null, "checkpoint-a"),
          row(checkpointSchema, "b", "dv-1", "checkpoint-b"),
          row(checkpointSchema, "c", null, "checkpoint-c")))
      writeJson(
        commit10,
        batch(
          jsonSchema,
          row(jsonSchema, "a", null, "json-a-v10", "add"),
          row(jsonSchema, "b", "dv-1", "json-b-v10", "add")))
      writeJson(
        commit11,
        batch(
          jsonSchema,
          row(jsonSchema, "a", null, "json-a-v11", "add"),
          row(jsonSchema, "b", "dv-1", null, "remove"),
          row(jsonSchema, "d", null, "json-d-v11", "add")))

      val commits = FileScan.json(
        Seq(scanFile(commit10, 10L), scanFile(commit11, 11L)).asJava,
        Optional.empty(),
        Collections.singletonList("version"),
        changesSchema)
      val latest = new Cte(
        1L,
        new Aggregate(
          commits,
          Seq[Expression](new Column("path"), new Column("dv")).asJava,
          Collections.singletonList(
            Agg.maxNonNullBy(
              new Column("live"),
              StringType.STRING,
              new Column("action"),
              StringType.STRING,
              new Column("version"),
              LongType.LONG)),
          checkpointSchema))
      val liveCommits = new Filter(latest, new Predicate("IS_NOT_NULL", new Column("live")))
      val checkpointScan = FileScan.parquet(
        Collections.singletonList(scanFile(checkpoint)),
        Optional.empty(),
        Collections.emptyList(),
        checkpointSchema,
        Optional.empty())
      val checkpointNotReplayed = new SemiJoin(
        checkpointScan,
        latest,
        Seq[Expression](new Column("path"), new Column("dv")).asJava,
        Seq[Expression](new Column("path"), new Column("dv")).asJava,
        Seq[DataType](StringType.STRING, StringType.STRING).asJava,
        true)
      val root = new UnionAll(Seq[PlanNode](liveCommits, checkpointNotReplayed).asJava)

      val cache = new PlanResultCache(4, Long.MaxValue)
      try {
        val result = new PlanExecutor(defaultEngine, cache).execute(root)
        assert(collect(result).sortBy(_._1) === Seq(
          ("a", None, "json-a-v11"),
          ("c", None, "checkpoint-c"),
          ("d", None, "json-d-v11")))
      } finally {
        cache.close()
      }
    }
  }

  private def writeJson(path: Path, data: ColumnarBatch): Unit =
    defaultEngine.getJsonHandler.writeJsonFileAtomically(path.toString, data.getRows, false)

  private def writeParquet(path: Path, data: ColumnarBatch): Unit =
    defaultEngine.getParquetHandler.writeParquetFileAtomically(
      path.toString,
      singletonCloseableIterator(data.toFiltered))

  private def batch(schema: StructType, rows: Row*): ColumnarBatch =
    new DefaultRowBasedColumnarBatch(schema, rows.asJava)

  private def row(schema: StructType, values: Object*): Row = {
    val present = new HashMap[Integer, Object]
    values.zipWithIndex.foreach { case (value, ordinal) =>
      if (value != null) present.put(Int.box(ordinal), value)
    }
    new GenericRow(schema, present)
  }

  private def scanFile(path: Path): ScanFile =
    new ScanFile(fileStatus(path), row(emptySchema), Optional.empty())

  private def scanFile(path: Path, version: Long): ScanFile =
    new ScanFile(
      fileStatus(path),
      row(versionSchema, Long.box(version)),
      Optional.empty())

  private def fileStatus(path: Path): FileStatus =
    FileStatus.of(
      path.toUri.toString,
      Files.size(path),
      Files.getLastModifiedTime(path).toMillis)

  private def collect(
      batches: io.delta.kernel.utils.CloseableIterator[ColumnarBatch])
      : Seq[(String, Option[String], String)] = {
    val output = ArrayBuffer.empty[(String, Option[String], String)]
    try {
      while (batches.hasNext) {
        val rows = batches.next().getRows
        try {
          while (rows.hasNext) {
            val row = rows.next()
            output += ((row.getString(0), Option(row.getString(1)), row.getString(2)))
          }
        } finally {
          rows.close()
        }
      }
    } finally {
      batches.close()
    }
    output.toSeq
  }
}
