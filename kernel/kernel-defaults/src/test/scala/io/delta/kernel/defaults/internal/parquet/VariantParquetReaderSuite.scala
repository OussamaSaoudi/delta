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
package io.delta.kernel.defaults.internal.parquet

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.data.{ColumnVector, FilteredColumnarBatch}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector
import io.delta.kernel.types.{ArrayType, MapType, StringType, StructType, VariantType}

import org.apache.parquet.io.api.{Binary, PrimitiveConverter}
import org.apache.parquet.schema.{GroupType, MessageTypeParser}
import org.scalatest.funsuite.AnyFunSuite

class VariantParquetReaderSuite extends AnyFunSuite with ParquetSuiteBase {

  test("Variant reader preserves reordered values, nulls, resizing, and selection") {
    val physicalType = variantGroup(
      """required binary metadata;
        |required binary value;""".stripMargin)
    val reader = new VariantColumnReader(1, physicalType)

    addVariant(reader, physicalType, Array[Byte](1), Array[Byte](11))
    reader.finalizeCurrentRow(0)
    reader.finalizeCurrentRow(1)
    addVariant(reader, physicalType, Array[Byte](2, 3), Array[Byte](12, 13))
    reader.finalizeCurrentRow(2)

    val vector = reader.getDataColumnVector(3)
    assert(vector.getDataType == VariantType.VARIANT)
    assert(vector.getVariant(0).getValue.sameElements(Array[Byte](1)))
    assert(vector.getVariant(0).getMetadata.sameElements(Array[Byte](11)))
    assert(vector.isNullAt(1))
    assert(vector.getVariant(2).getValue.sameElements(Array[Byte](2, 3)))
    assert(vector.getVariant(2).getMetadata.sameElements(Array[Byte](12, 13)))
    assertSelectedRows(vector)

    addVariant(reader, physicalType, Array[Byte](4), Array[Byte](14))
    reader.finalizeCurrentRow(3)
    val nextBatch = reader.getDataColumnVector(1)
    assert(nextBatch.getVariant(0).getValue.sameElements(Array[Byte](4)))
  }

  test("Variant reader rejects incomplete and duplicate physical values") {
    val physicalType = variantGroup(
      """required binary value;
        |required binary metadata;""".stripMargin)
    val missingMetadata = new VariantColumnReader(1, physicalType)
    missingMetadata.start()
    converter(missingMetadata, physicalType, "value")
      .addBinary(Binary.fromConstantByteArray(Array(1)))
    assert(intercept[IllegalArgumentException](missingMetadata.end()).getMessage
      .contains("metadata"))

    val duplicateValue = new VariantColumnReader(1, physicalType)
    duplicateValue.start()
    val values = converter(duplicateValue, physicalType, "value")
    values.addBinary(Binary.fromConstantByteArray(Array(1)))
    assert(intercept[IllegalArgumentException] {
      values.addBinary(Binary.fromConstantByteArray(Array(2)))
    }.getMessage.contains("more than once"))
  }

  test("Variant schema pruning preserves a valid unshredded group") {
    val fileSchema = MessageTypeParser.parseMessageType(
      """message fileSchema {
        |  optional group v {
        |    required binary metadata;
        |    required binary value;
        |  }
        |  optional int32 ignored;
        |}""".stripMargin)
    val pruned = ParquetSchemaUtils.pruneSchema(
      fileSchema,
      new StructType().add("v", VariantType.VARIANT))

    assert(pruned.getFieldCount == 1)
    assert(pruned.asGroupType().getType("v").asGroupType().getFieldCount == 2)
  }

  test("Variant readers reject shredded and malformed physical layouts") {
    val invalidChildren = Seq(
      "required binary value;",
      "required binary value; required binary metadata; optional int32 typed_value;",
      "required int32 value; required binary metadata;",
      "optional binary value; required binary metadata;")

    invalidChildren.foreach { children =>
      val error = intercept[IllegalArgumentException] {
        new VariantColumnReader(1, variantGroup(children))
      }
      assert(error.getMessage.contains("Variant"))
    }
  }

  test("schema pruning rejects shredded Variant at every nested position") {
    val cases = Seq(
      (
        """optional group v {
          |  required binary value;
          |  required binary metadata;
          |  optional int32 typed_value;
          |}""".stripMargin,
        new StructType().add("v", VariantType.VARIANT)),
      (
        """optional group s {
          |  optional group v {
          |    required binary value;
          |    required binary metadata;
          |    optional int32 typed_value;
          |  }
          |}""".stripMargin,
        new StructType().add("s", new StructType().add("v", VariantType.VARIANT))),
      (
        """optional group a (LIST) {
          |  repeated group list {
          |    optional group element {
          |      required binary value;
          |      required binary metadata;
          |      optional int32 typed_value;
          |    }
          |  }
          |}""".stripMargin,
        new StructType().add("a", new ArrayType(VariantType.VARIANT, true))),
      (
        """optional group m (MAP) {
          |  repeated group key_value {
          |    required binary key (STRING);
          |    optional group value {
          |      required binary value;
          |      required binary metadata;
          |      optional int32 typed_value;
          |    }
          |  }
          |}""".stripMargin,
        new StructType()
          .add("m", new MapType(StringType.STRING, VariantType.VARIANT, true))))

    cases.foreach { case (field, schema) =>
      val fileSchema = MessageTypeParser.parseMessageType(
        s"""message fileSchema {
           |$field
           |}""".stripMargin)
      val error = intercept[IllegalArgumentException] {
        ParquetSchemaUtils.pruneSchema(fileSchema, schema)
      }
      assert(error.getMessage.contains("unshredded Variant"))
    }
  }

  test("Kernel reads Spark unshredded Variant values at every nested position") {
    val path = getTestResourceFilePath(
      "spark-variant-stable-feature-checkpoint/" +
        "part-00000-5f6f82ed-28c5-4f4e-b358-93904826c84d-c000.snappy.parquet")
    val variantArray = new ArrayType(VariantType.VARIANT, true)
    val schema = new StructType()
      .add("v", VariantType.VARIANT)
      .add("array_of_variants", variantArray)
      .add("struct_of_variants", new StructType().add("v", VariantType.VARIANT))
      .add("map_of_variants", new MapType(StringType.STRING, VariantType.VARIANT, true))
      .add(
        "array_of_struct_of_variants",
        new ArrayType(new StructType().add("v", VariantType.VARIANT), true))
      .add(
        "struct_of_array_of_variants",
        new StructType().add("v", variantArray))

    val batches = readParquetUsingKernelAsColumnarBatches(path, schema)
    try {
      val batch = batches.head
      val expectedValue = Array[Byte](2, 1, 0, 0, 2, 12, 0)
      val expectedMetadata = Array[Byte](1, 1, 0, 1, 'k'.toByte)
      val variants = Seq(
        batch.getColumnVector(0).getVariant(0),
        batch.getColumnVector(1).getArray(0).getElements.getVariant(0),
        batch.getColumnVector(2).getChild(0).getVariant(0),
        batch.getColumnVector(3).getMap(0).getValues.getVariant(0),
        batch.getColumnVector(4).getArray(0).getElements.getChild(0).getVariant(0),
        batch.getColumnVector(5).getChild(0).getArray(0).getElements.getVariant(0))

      variants.foreach { variant =>
        assert(variant.getValue.sameElements(expectedValue))
        assert(variant.getMetadata.sameElements(expectedMetadata))
      }
    } finally {
      batches.foreach { batch =>
        (0 until batch.getSchema.length()).foreach { ordinal =>
          batch.getColumnVector(ordinal).close()
        }
      }
    }
  }

  private def assertSelectedRows(vector: ColumnVector): Unit = {
    val schema = new StructType().add("v", VariantType.VARIANT)
    val data = new DefaultColumnarBatch(3, schema, Array(vector))
    val selection = new DefaultBooleanVector(
      3,
      Optional.empty(),
      Array(false, true, true))
    val rows = new FilteredColumnarBatch(data, Optional.of(selection)).getRows
    try {
      val selected = rows.asScala.toSeq
      assert(selected.size == 2)
      assert(selected.head.isNullAt(0))
      assert(selected(1).getVariant(0).getValue.sameElements(Array[Byte](2, 3)))
    } finally rows.close()
  }

  private def variantGroup(children: String): GroupType =
    MessageTypeParser.parseMessageType(
      s"""message fileSchema {
         |  optional group v {
         |    ${children.replace("\n", "\n    ")}
         |  }
         |}""".stripMargin).asGroupType().getType("v").asGroupType()

  private def addVariant(
      reader: VariantColumnReader,
      physicalType: GroupType,
      value: Array[Byte],
      metadata: Array[Byte]): Unit = {
    reader.start()
    converter(reader, physicalType, "value")
      .addBinary(Binary.fromConstantByteArray(value))
    converter(reader, physicalType, "metadata")
      .addBinary(Binary.fromConstantByteArray(metadata))
    reader.end()
  }

  private def converter(
      reader: VariantColumnReader,
      physicalType: GroupType,
      fieldName: String): PrimitiveConverter =
    reader.getConverter(physicalType.getFieldIndex(fieldName)).asPrimitiveConverter()
}
