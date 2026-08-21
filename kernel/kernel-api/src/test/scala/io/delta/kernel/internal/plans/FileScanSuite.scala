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
package io.delta.kernel.internal.plans

import java.lang.{Long => LongJ}
import java.net.URI
import java.util

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.internal.actions.DeletionVectorDescriptor
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.types._
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll, Table}

class FileScanSuite extends AnyFunSuite {
  private val schema = new StructType()
    .add("id", LongType.LONG)
    .add("part", StringType.STRING, false)
    .addMetadataColumn("row_index", MetadataColumnSpec.ROW_INDEX)
  private val constantsSchema = new StructType(util.Arrays.asList(schema.get("part")))

  private def constants(value: String): Row =
    GenericRow.fromValues(constantsSchema, Seq(value).asJava)

  private def file(path: String, value: String = "a"): ScanFile =
    new ScanFile(FileStatus.of(path, 10, 20), constants(value))

  test("Parquet scan preserves its model and builder") {
    val first = file("file:///table/data")
    val second = file("file:///table/data", "b")
    val scan = new ScanParquet(Seq(first, second).asJava, Seq("part").asJava, schema)

    assert(scan.getFiles.asScala === Seq(first, second))
    assert(scan.getFileConstantColumns.asScala === Seq("part"))
    assert(scan.getSchema === schema)
    assert(scan.getFiles.get(1).getFileConstants.getString(0) === "b")
    assert(PlanBuilder.scanParquet(scan.getFiles, scan.getFileConstantColumns, schema)
      .build().getOutputSchema === schema)
  }

  test("ScanFile represents known and unresolved file metadata") {
    val deletionVector = new DeletionVectorDescriptor(
      DeletionVectorDescriptor.INLINE_DV_MARKER,
      "",
      util.Optional.empty[Integer](),
      0,
      0)
    val known = new ScanFile(
      FileStatus.of("file:///table/known", 10, 20),
      constants("a"),
      util.Optional.of(deletionVector))
    val unresolved = new ScanFile(
      "file:///table/unresolved",
      constants("b"),
      util.Optional.of(deletionVector))

    assert(known.getPath === "file:///table/known")
    assert(known.getKnownFileStatus.get.getSize === 10)
    assert(unresolved.getPath === "file:///table/unresolved")
    assert(unresolved.getKnownFileStatus.isEmpty)
    assertThrows[IllegalStateException](unresolved.getFileStatus)
    assert(unresolved.getDeletionVector.get eq deletionVector)
  }

  test("FileScan retains a shared deletion-vector root") {
    val root = URI.create("file:///table")
    val deletionVector = new DeletionVectorDescriptor(
      DeletionVectorDescriptor.PATH_DV_MARKER,
      "file:///table/dv.bin",
      util.Optional.of(Int.box(1)),
      1,
      1)
    val scanFile = new ScanFile(
      FileStatus.of("file:///table/data"),
      constants("a"),
      util.Optional.of(deletionVector))
    val scan = new ScanParquet(
      Seq(scanFile).asJava,
      Seq("part").asJava,
      schema,
      util.Optional.of(root))

    assert(scan.getDeletionVectorRoot.get === root)
    assertThrows[IllegalArgumentException] {
      new ScanParquet(
        util.Collections.emptyList(),
        util.Collections.emptyList(),
        schema,
        util.Optional.of(URI.create("relative")))
    }
    assertThrows[IllegalArgumentException] {
      new ScanParquet(
        Seq(scanFile).asJava,
        Seq("part").asJava,
        schema)
    }
  }

  test("Parquet scan defensively copies collections") {
    val files = new util.ArrayList[ScanFile](Seq(file("file:///table/data")).asJava)
    val names = new util.ArrayList[String](Seq("part").asJava)
    val scan = new ScanParquet(files, names, schema)
    files.clear()
    names.clear()

    assert(scan.getFiles.size() === 1)
    assert(scan.getFileConstantColumns.asScala === Seq("part"))
    assertThrows[UnsupportedOperationException](scan.getFiles.clear())
    assertThrows[UnsupportedOperationException](scan.getFileConstantColumns.clear())
  }

  test("Parquet scan rejects invalid file constants") {
    val invalidNames = Table(
      ("names", "message"),
      (Seq("missing"), "is not in the scan schema"),
      (Seq("part", "part"), "duplicate name `part`"),
      (Seq("row_index"), "is a metadata column"))
    forAll(invalidNames) { (names, message) =>
      val error = intercept[IllegalArgumentException] {
        new ScanParquet(util.Collections.emptyList(), names.asJava, schema)
      }
      assert(error.getMessage.contains(message))
    }

    val invalidRows = Table(
      ("row", "message"),
      (GenericRow.fromValues(new StructType(), Seq.empty[AnyRef].asJava): Row, "schema differs"),
      (GenericRow.fromValues(
        new StructType().add("part", LongType.LONG, false),
        Seq(LongJ.valueOf(1)).asJava): Row, "schema differs"),
      (GenericRow.fromValues(constantsSchema, Seq(null).asJava): Row,
        "null for non-nullable field `part`"))
    forAll(invalidRows) { (row, message) =>
      val error = intercept[IllegalArgumentException] {
        new ScanParquet(
          Seq(new ScanFile(FileStatus.of("file:///table/data"), row)).asJava,
          Seq("part").asJava,
          schema)
      }
      assert(error.getMessage.contains(message))
    }
  }

  test("Parquet scan requires zero inputs") {
    val source = new Values(schema, util.Collections.emptyList())
    val scan = new ScanParquet(Seq(file("file:///table/data")).asJava, Seq("part").asJava, schema)
    val error = intercept[IllegalArgumentException] {
      new Plan(Seq(
        new PlanNode(source, util.Collections.emptyList()),
        new PlanNode(scan, Seq(Int.box(0)).asJava)).asJava)
    }
    assert(error.getMessage.contains("requires no inputs"))
  }

  test("JSON scan builder retains its source format") {
    val plan = PlanBuilder.scanJson(
      Seq(file("file:///table/data")).asJava,
      Seq("part").asJava,
      schema).build()

    assert(plan.getNodes.get(0).getOperator.isInstanceOf[ScanJson])
    assert(plan.getOutputSchema === schema)
  }
}
