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
package io.delta.kernel.plans

import java.net.URI
import java.util

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.internal.actions.DeletionVectorDescriptor
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.types._
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

class FileScanSuite extends AnyFunSuite {
  private val schema = new StructType()
    .add("id", LongType.LONG)
    .add("part", StringType.STRING, false)
    .addMetadataColumn("row_index", MetadataColumnSpec.ROW_INDEX)
  private val constantsSchema = new StructType(util.Arrays.asList(schema.get("part")))

  private def constants(value: String): Row =
    GenericRow.fromValues(constantsSchema, Seq(value).asJava)

  private def file(path: String, value: String = "a"): ScanFile =
    new ScanFile(
      FileStatus.of(path, 10, 20),
      constants(value),
      util.Optional.empty[DeletionVectorDescriptor]())

  test("static scans preserve complete file identity and payload") {
    val first = file("file:///table/a.parquet")
    val second = file("file:///table/b.parquet", "b")
    val scan = new ScanParquet(
      Seq(first, second).asJava,
      util.Optional.of(URI.create("file:///table/")),
      Seq("part").asJava,
      schema,
      util.Optional.empty())

    assert(scan.files().asScala === Seq(first, second))
    assert(scan.fileConstantColumns().asScala === Seq("part"))
    assert(scan.outputSchema() === schema)
    assert(second.getFileStatus.getSize === 10)
    assert(second.getFileStatus.getModificationTime === 20)
    assert(second.getFileConstants.getString(0) === "b")
  }

  test("ScanFile requires absolute paths and positive sizes") {
    assertThrows[IllegalArgumentException] {
      new ScanFile(
        FileStatus.of("relative", 10, 20),
        constants("a"),
        util.Optional.empty())
    }
    assertThrows[IllegalArgumentException] {
      new ScanFile(
        FileStatus.of("file:///table/empty", 0, 20),
        constants("a"),
        util.Optional.empty())
    }
  }

  test("relative deletion vectors require one table root") {
    val descriptor = new DeletionVectorDescriptor(
      DeletionVectorDescriptor.UUID_DV_MARKER,
      "encoded",
      util.Optional.empty[Integer](),
      1,
      1)
    val scanFile = new ScanFile(
      FileStatus.of("file:///table/data", 10, 20),
      constants("a"),
      util.Optional.of(descriptor))

    assertThrows[IllegalArgumentException] {
      new ScanJson(
        Seq(scanFile).asJava,
        util.Optional.empty(),
        Seq("part").asJava,
        schema)
    }
    val scan = new ScanJson(
      Seq(scanFile).asJava,
      util.Optional.of(URI.create("file:///table/")),
      Seq("part").asJava,
      schema)
    assert(scan.tableRoot().get() === URI.create("file:///table/"))
  }

  test("scan collections are immutable and structurally equal") {
    val files = new util.ArrayList[ScanFile](Seq(file("file:///table/data")).asJava)
    val names = new util.ArrayList[String](Seq("part").asJava)
    val first = new ScanJson(files, util.Optional.empty(), names, schema)
    files.clear()
    names.clear()
    val equal = new ScanJson(
      Seq(file("file:///table/data")).asJava,
      util.Optional.empty(),
      Seq("part").asJava,
      schema)

    assert(first === equal)
    assert(first.hashCode() === equal.hashCode())
    assertThrows[UnsupportedOperationException](first.files().clear())
    assertThrows[UnsupportedOperationException](first.fileConstantColumns().clear())
  }

  test("scan validates file constant names and rows") {
    assertThrows[IllegalArgumentException] {
      new ScanJson(
        util.Collections.emptyList(),
        util.Optional.empty(),
        Seq("part", "part").asJava,
        schema)
    }
    val wrong = GenericRow.fromValues(new StructType(), Seq.empty[AnyRef].asJava)
    assertThrows[IllegalArgumentException] {
      new ScanJson(
        Seq(new ScanFile(
          FileStatus.of("file:///table/data", 10, 20),
          wrong,
          util.Optional.empty())).asJava,
        util.Optional.empty(),
        Seq("part").asJava,
        schema)
    }
  }
}
