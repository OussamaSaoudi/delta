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

import java.lang.{Integer => IntegerJ}
import java.net.URI
import java.util

import scala.jdk.CollectionConverters._

import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.actions.DeletionVectorDescriptor
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll, Table}

class LoadSuite extends AnyFunSuite {
  private val pathField = new StructField("path", StringType.STRING, false)
  private val sizeField = new StructField("size", LongType.LONG, true)
  private val recordsField = new StructField("num_records", LongType.LONG, true)
  private val dvField = new StructField("dv", DeletionVectorDescriptor.READ_SCHEMA, true)
  private val versionField = new StructField("version", LongType.LONG, true)

  private val inputSchema = struct(pathField, sizeField, recordsField, dvField, versionField)
  private val outputSchema = new StructType()
    .add("id", LongType.LONG)
    .add("version", LongType.LONG)

  private def struct(fields: StructField*): StructType = new StructType(fields.asJava)

  private def column(parts: String*): Column = new Column(parts.toArray)

  private def fileMeta(
      path: Column = column("path"),
      size: Column = column("size"),
      records: Column = column("num_records")): LoadColumnFileMeta =
    new LoadColumnFileMeta(path, size, records)

  private def load(
      output: StructType = outputSchema,
      fileType: FileType = FileType.PARQUET,
      baseUri: Option[URI] = Some(URI.create("s3://bucket/table/")),
      constants: Seq[String] = Seq("version"),
      meta: LoadColumnFileMeta = fileMeta(),
      dv: Column = column("dv")): Load =
    new Load(
      output,
      fileType,
      baseUri.map(util.Optional.of[URI]).getOrElse(util.Optional.empty[URI]()),
      constants.asJava,
      meta,
      dv)

  private def plan(operator: Load, input: StructType = inputSchema): Plan = {
    val source = new Values(input, util.Collections.emptyList())
    new Plan(Seq(
      new PlanNode(source, util.Collections.emptyList()),
      new PlanNode(operator, Seq(IntegerJ.valueOf(0)).asJava)).asJava)
  }

  private def replace(schema: StructType, replacement: StructField): StructType =
    new StructType(schema.fields.asScala
      .map(field => if (field.getName == replacement.getName) replacement else field)
      .asJava)

  private def drop(schema: StructType, name: String): StructType =
    new StructType(schema.fields.asScala.filterNot(_.getName == name).asJava)

  test("Load preserves configuration and produces its declared schema") {
    val formats = Table("format", FileType.PARQUET, FileType.JSON)
    forAll(formats) { format =>
      val operator = load(fileType = format)
      val built = plan(operator)

      assert(built.getOutputSchema === outputSchema)
      assert(operator.getSchema === outputSchema)
      assert(operator.getFileType === format)
      assert(operator.getBaseUri.get === URI.create("s3://bucket/table/"))
      assert(operator.getFileConstantColumns.asScala === Seq("version"))
      assert(operator.getFileMeta.getPathColumn === column("path"))
      assert(operator.getFileMeta.getFileSizeColumn === column("size"))
      assert(operator.getFileMeta.getNumRecordsColumn === column("num_records"))
      assert(operator.getDvColumn === column("dv"))
    }
  }

  test("Load supports nested metadata columns, no base URI, and no constants") {
    val nestedMeta = struct(pathField, sizeField, recordsField, dvField)
    val nestedInput = new StructType().add("file", nestedMeta, false)
    val operator = new Load(
      new StructType().add("id", LongType.LONG),
      FileType.JSON,
      fileMeta(column("file", "path"), column("file", "size"), column("file", "num_records")),
      column("file", "dv"))

    assert(plan(operator, nestedInput).getOutputSchema === operator.getSchema)
    assert(operator.getBaseUri.isEmpty)
    assert(operator.getFileConstantColumns.isEmpty)
  }

  test("Load validates every metadata column path, type, and nullability") {
    val cases = Table(
      ("case", "operator", "input", "message"),
      (
        "missing path",
        load(meta = fileMeta(path = column("missing"))),
        inputSchema,
        "Load path column"),
      (
        "path through scalar",
        load(meta = fileMeta(path = column("path", "nested"))),
        inputSchema,
        "Load path column"),
      (
        "case mismatch",
        load(meta = fileMeta(path = column("Path"))),
        inputSchema,
        "Load path column"),
      ("empty path", load(meta = fileMeta(path = column())), inputSchema, "Load path column"),
      (
        "path type",
        load(),
        replace(inputSchema, new StructField("path", LongType.LONG, false)),
        "must have type string"),
      (
        "path nullability",
        load(),
        replace(inputSchema, new StructField("path", StringType.STRING, true)),
        "nullable=false"),
      (
        "size type",
        load(),
        replace(inputSchema, new StructField("size", StringType.STRING, true)),
        "must have type long"),
      (
        "size nullability",
        load(),
        replace(inputSchema, new StructField("size", LongType.LONG, false)),
        "nullable=true"),
      (
        "record type",
        load(),
        replace(inputSchema, new StructField("num_records", StringType.STRING, true)),
        "must have type long"),
      (
        "record nullability",
        load(),
        replace(inputSchema, new StructField("num_records", LongType.LONG, false)),
        "nullable=true"),
      (
        "DV type",
        load(),
        replace(inputSchema, new StructField("dv", StringType.STRING, true)),
        "deletion vector"),
      (
        "DV nullability",
        load(),
        replace(inputSchema, new StructField("dv", DeletionVectorDescriptor.READ_SCHEMA, false)),
        "nullable=true"))

    forAll(cases) { (_, operator, input, message) =>
      val error = intercept[IllegalArgumentException] { plan(operator, input) }
      assert(error.getMessage.contains(message))
    }
  }

  test("Load validates file constants in its output schema") {
    val metadataOutput = new StructType()
      .add("id", LongType.LONG)
      .addMetadataColumn("version", MetadataColumnSpec.ROW_INDEX)
    val cases = Table(
      ("constants", "output", "message"),
      (Seq("version", "version"), outputSchema, "duplicate name `version`"),
      (Seq("missing"), outputSchema, "absent from output schema"),
      (Seq("version"), metadataOutput, "metadata column in output schema"))

    forAll(cases) { (constants, output, message) =>
      val error = intercept[IllegalArgumentException] {
        load(output = output, constants = constants)
      }
      assert(error.getMessage.contains(message))
    }
  }

  test("Load validates file constant sources and types") {
    val metadataInput = struct(
      pathField,
      sizeField,
      recordsField,
      dvField,
      StructField.createMetadataColumn("version", MetadataColumnSpec.ROW_INDEX))
    val cases = Table(
      ("input", "output", "message"),
      (drop(inputSchema, "version"), outputSchema, "absent from input schema"),
      (metadataInput, outputSchema, "metadata column in input schema"),
      (
        inputSchema,
        new StructType().add("id", LongType.LONG)
          .add("version", StringType.STRING),
        "has input type long"))

    forAll(cases) { (input, output, message) =>
      val error = intercept[IllegalArgumentException] { plan(load(output = output), input) }
      assert(error.getMessage.contains(message))
    }
  }

  test("Load requires an absolute base URI and exactly one input") {
    val uriError = intercept[IllegalArgumentException] {
      load(baseUri = Some(URI.create("relative/table")))
    }
    assert(uriError.getMessage.contains("base URI is not absolute"))

    val operator = load()
    val noInput = intercept[IllegalArgumentException] {
      operator.getOutputSchema(util.Collections.emptyList())
    }
    assert(noInput.getMessage.contains("requires one input, got 0"))

    val twoInputs = intercept[IllegalArgumentException] {
      operator.getOutputSchema(Seq(inputSchema, inputSchema).asJava)
    }
    assert(twoInputs.getMessage.contains("requires one input, got 2"))
  }

  test("Load defensively copies and exposes an immutable constant list") {
    val constants = new util.ArrayList[String](Seq("version").asJava)
    val operator = new Load(
      outputSchema,
      FileType.PARQUET,
      util.Optional.empty(),
      constants,
      fileMeta(),
      column("dv"))
    constants.clear()

    assert(operator.getFileConstantColumns.asScala === Seq("version"))
    assertThrows[UnsupportedOperationException](operator.getFileConstantColumns.clear())
  }
}
