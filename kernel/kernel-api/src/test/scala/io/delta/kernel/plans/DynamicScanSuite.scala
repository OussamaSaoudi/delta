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
import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.actions.DeletionVectorDescriptor
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class DynamicScanSuite extends AnyFunSuite {
  private val metadataSchema = new StructType()
    .add("path", StringType.STRING, false)
    .add("size", LongType.LONG, false)
    .add("modified", LongType.LONG, false)
    .add("dv", DeletionVectorDescriptor.READ_SCHEMA)
    .add("version", LongType.LONG)
  private val dataSchema = new StructType()
    .add("id", LongType.LONG)
    .add("version", LongType.LONG)

  private def source(schema: StructType = metadataSchema): Values =
    new Values(schema, util.Collections.emptyList[Row]())

  private def scan(
      input: PlanNode = source(),
      output: StructType = dataSchema,
      root: URI = URI.create("s3://bucket/table/"),
      constants: Seq[String] = Seq("version"),
      size: Column = new Column("size")): DynamicScan =
    new DynamicScan(
      input,
      output,
      FileType.PARQUET,
      root,
      constants.asJava,
      new Column("path"),
      size,
      new Column("modified"),
      new Column("dv"))

  test("DynamicScan preserves metadata mapping and delegates to a static schema") {
    val input = source()
    val dynamic = scan(input)

    assert(dynamic.fileMetadataInput() eq input)
    assert(dynamic.children().asScala === Seq(input))
    assert(dynamic.outputSchema() === dataSchema)
    assert(dynamic.fileType() === FileType.PARQUET)
    assert(dynamic.tableRoot() === URI.create("s3://bucket/table/"))
    assert(dynamic.fileConstantColumns().asScala === Seq("version"))
    assert(dynamic.pathColumn() === new Column("path"))
    assert(dynamic.fileSizeColumn() === new Column("size"))
    assert(dynamic.lastModifiedColumn() === new Column("modified"))
    assert(dynamic.deletionVectorColumn() === new Column("dv"))
  }

  test("DynamicScan validates root, metadata columns, and constants") {
    assertThrows[IllegalArgumentException](scan(root = URI.create("relative")))
    assertThrows[IllegalArgumentException](scan(root = URI.create("s3://bucket/table")))
    assertThrows[IllegalArgumentException](scan(size = new Column("missing")))

    val wrongSize = new StructType()
      .add("path", StringType.STRING, false)
      .add("size", StringType.STRING, false)
      .add("modified", LongType.LONG, false)
      .add("dv", DeletionVectorDescriptor.READ_SCHEMA)
      .add("version", LongType.LONG)
    assertThrows[IllegalArgumentException](scan(input = source(wrongSize)))
    assertThrows[IllegalArgumentException](scan(constants = Seq("missing")))
    assertThrows[IllegalArgumentException](scan(constants = Seq("version", "version")))
  }

  test("DynamicScan is structurally equal but preserves explicit identity") {
    val first = scan()
    val equal = scan()

    assert(first === equal)
    assert(first.hashCode() === equal.hashCode())
    assert(!(first.fileMetadataInput() eq equal.fileMetadataInput()))
  }
}
