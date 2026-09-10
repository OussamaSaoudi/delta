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
package io.delta.kernel.internal.data

import java.lang.{Integer => IntegerJ, Long => LongJ}
import java.util

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class GenericDataSuite extends AnyFunSuite {

  test("GenericRow constructs a dense row and copies its input") {
    val schema = new StructType()
      .add("id", LongType.LONG, false)
      .add("name", StringType.STRING)
    val values = new util.ArrayList[AnyRef]()
    values.add(LongJ.valueOf(17L))
    values.add(null)

    val row = GenericRow.fromValues(schema, values)
    values.set(0, LongJ.valueOf(99L))

    assert(row.getSchema === schema)
    assert(row.getLong(0) === 17L)
    assert(row.isNullAt(1))
    assertThrows[IllegalArgumentException](row.isNullAt(-1))
    assertThrows[IllegalArgumentException](row.isNullAt(2))
  }

  test("GenericRow rejects dense values with the wrong width") {
    val schema = new StructType().add("id", LongType.LONG)
    val error = intercept[IllegalArgumentException] {
      GenericRow.fromValues(schema, Seq.empty[AnyRef].asJava)
    }

    assert(error.getMessage.contains("Expected 1 values"))
  }

  test("GenericRow constructs a dense row from owned values") {
    val schema = new StructType()
      .add("id", LongType.LONG, false)
      .add("name", StringType.STRING)

    val row = GenericRow.fromOwnedValues(schema, Array[AnyRef](LongJ.valueOf(17L), null))

    assert(row.getLong(0) === 17L)
    assert(row.isNullAt(1))
  }

  test("GenericColumnVector exposes null children for a null struct") {
    val structType = new StructType().add("value", IntegerType.INTEGER)
    val row: Row = GenericRow.fromValues(structType, Seq(IntegerJ.valueOf(7)).asJava)
    val values = new util.ArrayList[Row]()
    values.add(null)
    values.add(row)

    val vector = new GenericColumnVector(values, structType)
    val child = vector.getChild(0)

    assert(child.getDataType === IntegerType.INTEGER)
    assert(child.getSize === 2)
    assert(child.isNullAt(0))
    assert(child.getInt(1) === 7)
    assert(child eq vector.getChild(0))
  }

  test("RowBackedColumnarBatch appends a column as a vector view") {
    val inputSchema = new StructType().add("id", IntegerType.INTEGER, false)
    val input = GenericRow.fromValues(inputSchema, Seq(IntegerJ.valueOf(7)).asJava)
    val appendedField = new StructField("count", LongType.LONG, false)
    val appendedValues = new GenericColumnVector(Seq(LongJ.valueOf(11L)).asJava, LongType.LONG)

    val output = new RowBackedColumnarBatch(inputSchema, Seq(input).asJava)
      .withNewColumn(1, appendedField, appendedValues)

    assert(output.getSchema === inputSchema.add(appendedField))
    assert(output.getColumnVector(0).getInt(0) === 7)
    assert(output.getColumnVector(1).getLong(0) === 11L)
    val rows = output.getRows
    try {
      val row = rows.next()
      assert(row.getSchema === output.getSchema)
      assert(row.getInt(0) === 7)
      assert(row.getLong(1) === 11L)
      assert(!rows.hasNext)
    } finally {
      rows.close()
    }
  }

  test("generic rows and struct children use logical Kernel getters") {
    val geometryType = GeometryType.ofDefault()
    val geographyType = GeographyType.ofDefault()
    val schema = new StructType()
      .add("date", DateType.DATE, false)
      .add("timestamp", TimestampType.TIMESTAMP, false)
      .add("timestampNtz", TimestampNTZType.TIMESTAMP_NTZ, false)
      .add("geometry", geometryType, false)
      .add("geography", geographyType, false)
    val row = GenericRow.fromValues(
      schema,
      Seq[AnyRef](
        IntegerJ.valueOf(12),
        LongJ.valueOf(34L),
        LongJ.valueOf(56L),
        "POINT (0 0)",
        "POINT (1 1)").asJava)
    val structVector = new GenericColumnVector(Seq(row).asJava, schema)
    val children = (0 until schema.length()).map(structVector.getChild)
    val cases = Seq[(String, () => Any, () => Any, Any)](
      ("date", () => row.getInt(0), () => children(0).getInt(0), 12),
      ("timestamp", () => row.getLong(1), () => children(1).getLong(0), 34L),
      ("timestampNtz", () => row.getLong(2), () => children(2).getLong(0), 56L),
      ("geometry", () => row.getString(3), () => children(3).getString(0), "POINT (0 0)"),
      ("geography", () => row.getString(4), () => children(4).getString(0), "POINT (1 1)"))

    cases.foreach { case (name, readRow, readVector, expected) =>
      withClue(name) {
        assert(readRow() === expected)
        assert(readVector() === expected)
      }
    }
    children.foreach(_.close())
  }

  test("GenericRow sparse constructor accepts inferred Scala map value types") {
    val schema = new StructType().add("value", IntegerType.INTEGER)
    val values = util.Collections.singletonMap(IntegerJ.valueOf(0), IntegerJ.valueOf(7))

    val row = new GenericRow(schema, values)

    assert(row.getInt(0) === 7)
  }
}
