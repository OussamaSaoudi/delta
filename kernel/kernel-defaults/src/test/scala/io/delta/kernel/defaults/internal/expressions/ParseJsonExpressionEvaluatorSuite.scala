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

import io.delta.kernel.data.{ColumnarBatch, ColumnVector}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.{DefaultGenericVector, DefaultStructVector}
import io.delta.kernel.expressions.{Column, Expression, ParseJson}
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class ParseJsonExpressionEvaluatorSuite extends AnyFunSuite {
  private val inputSchema = new StructType().add("json", StringType.STRING, true)

  private def jsonBatch(values: Seq[String]): ColumnarBatch =
    new DefaultColumnarBatch(
      values.size,
      inputSchema,
      Array(DefaultGenericVector.fromArray(StringType.STRING, values.toArray[AnyRef])))

  private def evaluate(
      input: ColumnarBatch,
      expression: Expression,
      outputType: DataType): ColumnVector =
    new DefaultExpressionEvaluator(input.getSchema, expression, outputType).eval(input)

  test("parses Kernel-native primitive and complex values") {
    val nestedType = new StructType().add("x", IntegerType.INTEGER, true)
    val arrayType = new ArrayType(LongType.LONG, true)
    val mapType = new MapType(StringType.STRING, BooleanType.BOOLEAN, true)
    val outputSchema = new StructType()
      .add("id", LongType.LONG, true)
      .add("name", StringType.STRING, true)
      .add("nested", nestedType, true)
      .add("items", arrayType, true)
      .add("flags", mapType, true)
    val input = jsonBatch(Seq(
      """{"id":1,"name":"one","nested":{"x":2},"items":[3,null],"flags":{"a":true}}""",
      """{"id":4,"ignored":"extra"}""",
      null))

    val result = evaluate(input, new ParseJson(new Column("json"), outputSchema), outputSchema)

    assert(result.getDataType == outputSchema)
    assert(result.isInstanceOf[DefaultStructVector])
    assert(result.getChild(2).isInstanceOf[DefaultStructVector])
    assert((0 until 3).forall(rowId => !result.isNullAt(rowId)))
    assert(result.getChild(0).getLong(0) == 1L)
    assert(result.getChild(1).getString(0) == "one")
    assert(result.getChild(2).getChild(0).getInt(0) == 2)
    assert(result.getChild(3).getArray(0).getElements.getLong(0) == 3L)
    assert(result.getChild(3).getArray(0).getElements.isNullAt(1))
    assert(result.getChild(4).getMap(0).getValues.getBoolean(0))
    assert(result.getChild(0).getLong(1) == 4L)
    assert((1 until outputSchema.length()).forall(result.getChild(_).isNullAt(1)))
    assert((0 until outputSchema.length()).forall(result.getChild(_).isNullAt(2)))
    result.close()
  }

  test("empty input produces an empty struct vector") {
    val outputSchema = new StructType().add("value", LongType.LONG, true)
    val result = evaluate(
      jsonBatch(Seq.empty),
      new ParseJson(new Column("json"), outputSchema),
      outputSchema)

    assert(result.getDataType == outputSchema)
    assert(result.getSize == 0)
    assert(result.isInstanceOf[DefaultStructVector])
    assert(result.getChild(0).getSize == 0)
    result.close()
    result.close()
  }

  Seq[(String, StructType, Seq[String])](
    (
      "empty string",
      new StructType().add("value", LongType.LONG, true),
      Seq("""{"value":1}""", "")),
    (
      "malformed object",
      new StructType().add("value", LongType.LONG, true),
      Seq("""{"value":1}""", """{"value":2""")),
    (
      "multiple objects",
      new StructType().add("value", LongType.LONG, true),
      Seq("""{"value":1}""", """{"value":2}{"value":3}""")),
    (
      "strict leaf mismatch",
      new StructType().add("value", LongType.LONG, true),
      Seq("""{"value":1}""", """{"value":"bad"}""")),
    (
      "missing required field",
      new StructType().add("value", LongType.LONG, false),
      Seq("""{"value":1}""", "{}")),
    (
      "invalid required failure-prone leaf",
      new StructType().add("value", DateType.DATE, false),
      Seq("""{"value":"1970-01-01"}""", """{"value":"bad"}"""))).foreach {
    case (name, outputSchema, values) =>
      test(s"a $name falls back to an all-null struct batch") {
        val result = evaluate(
          jsonBatch(values),
          new ParseJson(new Column("json"), outputSchema),
          outputSchema)

        assert(result.getSize == values.size)
        assert((0 until result.getSize).forall(result.isNullAt))
        assert((0 until result.getSize).forall(result.getChild(0).isNullAt))
        result.close()
      }
  }

  test("invalid failure-prone leaves become per-cell nulls through nested structs") {
    val leafType = new StructType()
      .add("date", DateType.DATE, true)
      .add("timestamp", TimestampType.TIMESTAMP, true)
      .add("timestampNtz", TimestampNTZType.TIMESTAMP_NTZ, true)
      .add("decimal", new DecimalType(5, 2), true)
      .add("id", LongType.LONG, false)
    val outputSchema = new StructType().add("stats", leafType, false)
    val input = jsonBatch(Seq(
      """{"stats":{"date":"1970-01-02","timestamp":"1970-01-01T00:00:01Z",""" +
        """"timestampNtz":"1970-01-01T00:00:02","decimal":"10.50","id":1}}""",
      """{"stats":{"date":"bad","timestamp":"bad","timestampNtz":"bad",""" +
        """"decimal":"999999.00","id":2}}""",
      """{"stats":{"date":"1970-01-03","timestamp":"1970-01-01T00:00:03Z",""" +
        """"timestampNtz":"1970-01-01T00:00:04","decimal":"12.345","id":3}}""",
      """{"stats":{"date":"1970-01-04","timestamp":"+48690-07-02T22:50:38.211Z",""" +
        """"timestampNtz":"1970-01-01T00:00:05","decimal":"13.50","id":4}}"""))

    val result = evaluate(input, new ParseJson(new Column("json"), outputSchema), outputSchema)
    val stats = result.getChild(0)

    assert((0 until 4).forall(rowId => !result.isNullAt(rowId) && !stats.isNullAt(rowId)))
    assert(stats.getChild(0).getInt(0) == 1)
    assert(stats.getChild(1).getLong(0) == 1000000L)
    assert(stats.getChild(2).getLong(0) == 2000000L)
    assert(stats.getChild(3).getDecimal(0) == new JBigDecimal("10.50"))
    assert((0 until 4).forall(stats.getChild(_).isNullAt(1)))
    assert(stats.getChild(4).getLong(1) == 2L)
    assert(stats.getChild(0).getInt(2) == 2)
    assert(stats.getChild(1).getLong(2) == 3000000L)
    assert(stats.getChild(2).getLong(2) == 4000000L)
    assert(stats.getChild(3).getDecimal(2) == new JBigDecimal("12.35"))
    assert(stats.getChild(0).getInt(3) == 3)
    assert(stats.getChild(1).isNullAt(3))
    assert(stats.getChild(2).getLong(3) == 5000000L)
    assert(stats.getChild(3).getDecimal(3) == new JBigDecimal("13.50"))
    assert(stats.getChild(4).getLong(3) == 4L)
    result.close()
    result.close()
  }

  test("duplicate schema names retain the existing tree-decoder fallback") {
    val outputSchema = new StructType()
      .add("value", IntegerType.INTEGER, true)
      .add("value", IntegerType.INTEGER, true)
    val result = evaluate(
      jsonBatch(Seq("""{"value":7}""")),
      new ParseJson(new Column("json"), outputSchema),
      outputSchema)

    assert(result.getChild(0).getInt(0) == 7)
    assert(result.getChild(1).getInt(0) == 7)
    result.close()
  }

  Seq[(String, DataType, String)](
    ("array", new ArrayType(LongType.LONG, true), """[1,"bad"]"""),
    ("map", new MapType(StringType.STRING, LongType.LONG, true), """{"a":"bad"}""")).foreach {
    case (name, fieldType, value) =>
      test(s"invalid $name children fail the containing value") {
        val outputSchema = new StructType().add("value", fieldType, true)
        val result = evaluate(
          jsonBatch(Seq(s"""{"value":$value}""")),
          new ParseJson(new Column("json"), outputSchema),
          outputSchema)

        assert(result.isNullAt(0))
        assert(result.getChild(0).isNullAt(0))
        result.close()
      }
  }

  test("streaming parser preserves rightmost duplicate fields") {
    val nested = new StructType().add("x", IntegerType.INTEGER, false)
    val outputSchema = new StructType()
      .add("value", IntegerType.INTEGER, false)
      .add("nested", nested, false)
    val valid = evaluate(
      jsonBatch(Seq(
        """{"value":"bad","value":7,"nested":{"x":"bad"},"nested":{"x":2}}""")),
      new ParseJson(new Column("json"), outputSchema),
      outputSchema)

    assert(valid.getChild(0).getInt(0) == 7)
    assert(valid.getChild(1).getChild(0).getInt(0) == 2)
    valid.close()

    val invalid = evaluate(
      jsonBatch(Seq("""{"value":7,"value":"bad","nested":{"x":2}}""")),
      new ParseJson(new Column("json"), outputSchema),
      outputSchema)
    assert(invalid.isNullAt(0))
    invalid.close()
  }

  test("streaming parser isolates JSON row boundaries") {
    val outputSchema = new StructType().add("value", IntegerType.INTEGER, true)
    val result = evaluate(
      jsonBatch(Seq(
        """{"value":1}],[{"value":2}""",
        """{"value":3}""")),
      new ParseJson(new Column("json"), outputSchema),
      outputSchema)

    assert((0 until result.getSize).forall(result.isNullAt))
    result.close()
  }

  Seq[(String, StructType, Expression, DataType)](
    (
      "non-string input",
      new StructType().add("json", IntegerType.INTEGER),
      new ParseJson(new Column("json"), new StructType()),
      new StructType()),
    (
      "non-struct output",
      inputSchema,
      new ParseJson(new Column("json"), new StructType()),
      LongType.LONG),
    (
      "mismatched output schema",
      inputSchema,
      new ParseJson(new Column("json"), new StructType().add("value", LongType.LONG)),
      new StructType().add("renamed", LongType.LONG))).foreach {
    case (name, schema, expression, outputType) =>
      test(s"rejects $name during evaluator construction") {
        intercept[UnsupportedOperationException] {
          new DefaultExpressionEvaluator(schema, expression, outputType)
        }
      }
  }
}
