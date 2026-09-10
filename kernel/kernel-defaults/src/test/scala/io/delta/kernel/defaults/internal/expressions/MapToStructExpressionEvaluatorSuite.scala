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
package io.delta.kernel.defaults.internal.expressions

import java.math.{BigDecimal => JBigDecimal}
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.{Date, Timestamp}
import java.util
import java.util.Optional

import io.delta.kernel.data.{ColumnarBatch, ColumnVector}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.{DefaultGenericVector, DefaultMapVector}
import io.delta.kernel.defaults.utils.DefaultKernelTestUtils.getValueAsObject
import io.delta.kernel.expressions.{Column, Expression, MapToStruct, ScalarExpression}
import io.delta.kernel.internal.util.InternalUtils
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class MapToStructExpressionEvaluatorSuite extends AnyFunSuite {
  private val mapType = new MapType(StringType.STRING, StringType.STRING, true)
  private val mapSchema = new StructType().add("partitionValues", mapType, true)

  private def mapVector(rows: Seq[Option[Seq[(String, String)]]]): ColumnVector = {
    val keys = rows.flatMap(_.toSeq.flatten.map(_._1))
    val values = rows.flatMap(_.toSeq.flatten.map(_._2))
    val offsets = rows.scanLeft(0)((offset, row) => offset + row.fold(0)(_.size)).toArray
    val nullability = rows.map(_.isEmpty).toArray
    new DefaultMapVector(
      rows.size,
      mapType,
      Optional.of(nullability),
      offsets,
      DefaultGenericVector.fromArray(StringType.STRING, keys.toArray[AnyRef]),
      DefaultGenericVector.fromArray(StringType.STRING, values.toArray[AnyRef]))
  }

  private def mapBatch(rows: Seq[Option[Seq[(String, String)]]]): ColumnarBatch =
    new DefaultColumnarBatch(rows.size, mapSchema, Array(mapVector(rows)))

  private def evaluate(
      input: ColumnarBatch,
      expression: Expression,
      outputType: DataType): ColumnVector =
    new DefaultExpressionEvaluator(input.getSchema, expression, outputType).eval(input)

  private val nonAsciiText: String = "h" + 233.toChar + "llo"

  private val primitiveCases = Seq[(String, DataType, String, Any)](
    ("boolean", BooleanType.BOOLEAN, "TrUe", true),
    ("byte", ByteType.BYTE, "-8", -8.toByte),
    ("short", ShortType.SHORT, "1200", 1200.toShort),
    ("integer", IntegerType.INTEGER, "42", 42),
    ("long", LongType.LONG, "123456789012", 123456789012L),
    ("float", FloatType.FLOAT, "1.25", 1.25f),
    ("double", DoubleType.DOUBLE, "2.5", 2.5d),
    ("decimal", new DecimalType(8, 2), "123.45", new JBigDecimal("123.45")),
    (
      "date",
      DateType.DATE,
      "2024-01-15",
      InternalUtils.daysSinceEpoch(Date.valueOf("2024-01-15"))),
    (
      "timestamp with offset",
      TimestampType.TIMESTAMP,
      "2024-06-15T14:30:00+05:00",
      1718443800000000L),
    (
      "timestamp_ntz",
      TimestampNTZType.TIMESTAMP_NTZ,
      "2024-01-15 12:34:56.789123",
      InternalUtils.microsSinceEpoch(Timestamp.valueOf("2024-01-15 12:34:56.789123"))),
    ("string", StringType.STRING, "hello", "hello"),
    ("binary", BinaryType.BINARY, nonAsciiText, nonAsciiText.getBytes(UTF_8)))

  primitiveCases.foreach { case (name, dataType, serialized, expected) =>
    test(s"parses $name fields") {
      val outputType = new StructType().add("value", dataType, true)
      val input = mapBatch(Seq(Some(Seq("value" -> serialized))))
      val result = evaluate(input, new MapToStruct(new Column("partitionValues")), outputType)
      val actual = getValueAsObject(result.getChild(0), 0)

      expected match {
        case bytes: Array[Byte] => assert(actual.asInstanceOf[Array[Byte]].sameElements(bytes))
        case _ => assert(actual == expected)
      }
      result.close()
    }
  }

  test("applies map projection, duplicate, empty-string, and null semantics") {
    val outputType = new StructType()
      .add("region", StringType.STRING, true)
      .add("blob", BinaryType.BINARY, true)
      .add("count", IntegerType.INTEGER, true)
    val input = mapBatch(Seq(
      Some(Seq(
        "region" -> "first",
        "ignored" -> "x",
        "count" -> "invalid",
        "region" -> "last",
        "count" -> "7")),
      Some(Seq("region" -> "", "blob" -> "", "count" -> "")),
      Some(Seq("region" -> "earlier", "region" -> null)),
      None))

    val result = evaluate(input, new MapToStruct(new Column("partitionValues")), outputType)
    assert(result.getChild(0).getString(0) == "last")
    assert(result.getChild(1).isNullAt(0))
    assert(result.getChild(2).getInt(0) == 7)
    assert(result.getChild(0).getString(1) == "")
    assert(result.getChild(1).getBinary(1).isEmpty)
    assert(result.getChild(2).isNullAt(1))
    assert(result.getChild(0).isNullAt(2))
    assert(!result.isNullAt(2))
    assert(result.isNullAt(3))
    assert((0 until outputType.length()).forall(result.getChild(_).isNullAt(3)))
    result.close()
  }

  test("resolves MapToStruct inside coalesce") {
    val outputType = new StructType().add("region", StringType.STRING, false)
    val rows = Seq(Some(Seq("region" -> "us")), None)
    val parsed = DefaultGenericVector.fromArray(outputType, Array[AnyRef](null, null))
    val inputSchema = new StructType()
      .add("parsed", outputType, true)
      .add("partitionValues", mapType, true)
    val input = new DefaultColumnarBatch(2, inputSchema, Array(parsed, mapVector(rows)))
    val expression = new ScalarExpression(
      "COALESCE",
      util.Arrays.asList(
        new Column("parsed"),
        new MapToStruct(new Column("partitionValues"))))

    val result = evaluate(input, expression, outputType)
    assert(result.getChild(0).getString(0) == "us")
    assert(result.isNullAt(1))
    result.close()
  }

  test("propagates partition parse failures") {
    val outputType = new StructType().add("count", IntegerType.INTEGER, true)
    val input = mapBatch(Seq(Some(Seq("count" -> "not_a_number"))))
    val error = intercept[NumberFormatException] {
      evaluate(input, new MapToStruct(new Column("partitionValues")), outputType)
    }
    assert(error.getMessage.contains("not_a_number"))
  }

  test("rejects invalid boolean values") {
    val outputType = new StructType().add("enabled", BooleanType.BOOLEAN, true)
    val input = mapBatch(Seq(Some(Seq("enabled" -> "yes"))))
    val error = intercept[IllegalArgumentException] {
      evaluate(input, new MapToStruct(new Column("partitionValues")), outputType)
    }
    assert(error.getMessage.contains("yes"))
  }

  test("rejects an unmasked null in a non-nullable output field") {
    val outputType = new StructType().add("required", IntegerType.INTEGER, false)
    val input = mapBatch(Seq(Some(Seq.empty), None))
    val error = intercept[IllegalArgumentException] {
      evaluate(input, new MapToStruct(new Column("partitionValues")), outputType)
    }
    assert(error.getMessage.contains("required"))
  }

  Seq[(String, StructType, DataType)](
    (
      "non-map input",
      new StructType().add("partitionValues", StringType.STRING, true),
      new StructType().add("value", StringType.STRING, true)),
    (
      "non-string map input",
      new StructType().add(
        "partitionValues",
        new MapType(StringType.STRING, IntegerType.INTEGER, true),
        true),
      new StructType().add("value", StringType.STRING, true)),
    ("non-struct output", mapSchema, StringType.STRING),
    (
      "complex output field",
      mapSchema,
      new StructType().add("value", new ArrayType(StringType.STRING, true), true)))
    .foreach { case (name, inputSchema, outputType) =>
      test(s"rejects $name") {
        val error = intercept[UnsupportedOperationException] {
          new DefaultExpressionEvaluator(
            inputSchema,
            new MapToStruct(new Column("partitionValues")),
            outputType)
        }
        assert(error.getMessage.contains("MapToStruct"))
      }
    }

  test("supports empty input and output schemas") {
    val result = evaluate(
      mapBatch(Seq.empty),
      new MapToStruct(new Column("partitionValues")),
      new StructType())
    assert(result.getSize == 0)
    assert(result.getDataType == new StructType())
  }
}
