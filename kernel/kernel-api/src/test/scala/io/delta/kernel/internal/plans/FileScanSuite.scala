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
  private type ScanFactory =
    (util.List[ScanFile], util.List[String], StructType) => FileScan

  private val scanFactories = Table(
    ("format", "factory"),
    ("Parquet", (files, constants, schema) => new ScanParquet(files, constants, schema)),
    ("JSON", (files, constants, schema) => new ScanJson(files, constants, schema)))

  private val schema = new StructType()
    .add("id", LongType.LONG)
    .add("part", StringType.STRING, false)
    .addMetadataColumn("row_index", MetadataColumnSpec.ROW_INDEX)
  private val constantsSchema = new StructType(util.Arrays.asList(schema.get("part")))

  private def constants(value: String): Row =
    GenericRow.fromValues(constantsSchema, Seq(value).asJava)

  private def file(path: String, value: String = "a"): ScanFile =
    new ScanFile(FileStatus.of(path, 10, 20), constants(value))

  test("scan sources preserve files, constants, schema, and repeated paths") {
    forAll(scanFactories) { (_, factory: ScanFactory) =>
      val first = file("file:///table/part=a/data-1")
      val second = file("file:///table/part=a/data-1", "b")
      val scan = factory(Seq(first, second).asJava, Seq("part").asJava, schema)
      val plan = new Plan(Seq(new PlanNode(scan, util.Collections.emptyList())).asJava)

      assert(scan.getFiles.asScala === Seq(first, second))
      assert(scan.getFileConstantColumns.asScala === Seq("part"))
      assert(scan.getSchema === schema)
      assert(plan.getOutputSchema === schema)
      assert(scan.getFiles.get(1).getFileConstants.getString(0) === "b")
    }
  }

  test("scan sources accept no files and no constants") {
    forAll(scanFactories) { (_, factory: ScanFactory) =>
      val scan = factory(
        util.Collections.emptyList(),
        util.Collections.emptyList(),
        schema)
      assert(scan.getFiles.isEmpty)
      assert(scan.getFileConstantColumns.isEmpty)
    }
  }

  test("scan model defensively copies collections and file status") {
    forAll(scanFactories) { (_, factory: ScanFactory) =>
      val status = FileStatus.of("file:///table/data", 10, 20)
      val scanFile = new ScanFile(status, constants("a"))
      val files = new util.ArrayList[ScanFile](Seq(scanFile).asJava)
      val names = new util.ArrayList[String](Seq("part").asJava)
      val scan = factory(files, names, schema)
      files.clear()
      names.clear()

      assert(scanFile.getFileStatus ne status)
      assert(scanFile.getFileStatus === status)
      assert(scan.getFiles.size() === 1)
      assert(scan.getFileConstantColumns.asScala === Seq("part"))
      assertThrows[UnsupportedOperationException](scan.getFiles.clear())
      assertThrows[UnsupportedOperationException](scan.getFileConstantColumns.clear())
    }
  }

  test("scan sources reject invalid file-constant names") {
    val cases = Table(
      ("names", "message"),
      (Seq("missing"), "is not in the scan schema"),
      (Seq("part", "part"), "duplicate name `part`"),
      (Seq("row_index"), "is a metadata column"))

    forAll(scanFactories) { (_, factory: ScanFactory) =>
      forAll(cases) { (names, message) =>
        val error = intercept[IllegalArgumentException] {
          factory(util.Collections.emptyList(), names.asJava, schema)
        }
        assert(error.getMessage.contains(message))
      }
    }
  }

  test("scan sources validate file-constant count, type, and nullability") {
    val wrongCount = GenericRow.fromValues(new StructType(), Seq.empty[AnyRef].asJava)
    val wrongTypeSchema = new StructType().add("part", LongType.LONG, false)
    val wrongType = GenericRow.fromValues(wrongTypeSchema, Seq(LongJ.valueOf(1)).asJava)
    val nullValue = GenericRow.fromValues(constantsSchema, Seq(null).asJava)
    val cases = Table(
      ("row", "message"),
      (wrongCount: Row, "has 0 value(s)"),
      (wrongType: Row, "schema differs"),
      (nullValue: Row, "null for non-nullable field `part`"))

    forAll(scanFactories) { (_, factory: ScanFactory) =>
      forAll(cases) { (row, message) =>
        val scanFile = new ScanFile(FileStatus.of("file:///table/data"), row)
        val error = intercept[IllegalArgumentException] {
          factory(Seq(scanFile).asJava, Seq("part").asJava, schema)
        }
        assert(error.getMessage.contains(message))
      }
    }
  }

  test("scan sources require zero inputs") {
    forAll(scanFactories) { (_, factory: ScanFactory) =>
      val scan = factory(Seq(file("file:///table/data")).asJava, Seq("part").asJava, schema)
      val source = new Values(schema, util.Collections.emptyList())
      val error = intercept[IllegalArgumentException] {
        new Plan(Seq(
          new PlanNode(source, util.Collections.emptyList()),
          new PlanNode(scan, Seq(Int.box(0)).asJava)).asJava)
      }
      assert(error.getMessage.contains("requires no inputs"))
    }
  }

  test("ScanFile validates file metadata") {
    val invalidStatuses = Table(
      ("status", "message"),
      (FileStatus.of("relative/path"), "not an absolute URI"),
      (FileStatus.of("file:///bad path"), "not a valid URI"),
      (FileStatus.of("file:///table/data", -1, 0), "size must be non-negative"))

    forAll(invalidStatuses) { (status, message) =>
      val error = intercept[IllegalArgumentException] {
        new ScanFile(status)
      }
      assert(error.getMessage.contains(message))
    }
  }

  test("ScanFile represents known and unresolved status with an optional deletion vector") {
    val deletionVector = new DeletionVectorDescriptor(
      DeletionVectorDescriptor.INLINE_DV_MARKER,
      "inline",
      util.Optional.empty(),
      1,
      1)
    val unresolved = new ScanFile(
      "file:///table/data",
      constants("a"),
      util.Optional.of(deletionVector))

    assert(unresolved.getPath === "file:///table/data")
    assert(unresolved.getKnownFileStatus.isEmpty)
    assert(unresolved.getDeletionVector.get() eq deletionVector)
    assertThrows[IllegalStateException](unresolved.getFileStatus)

    val status = FileStatus.of("file:///table/data", 10, 20)
    val known = new ScanFile(status, constants("a"), util.Optional.of(deletionVector))
    assert(known.getKnownFileStatus.get() === status)
    assert(known.getFileStatus === status)
  }
}
