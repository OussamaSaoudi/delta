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

import java.lang.{Double => DoubleJ, Float => FloatJ, Integer => IntegerJ, Long => LongJ}
import java.math.{BigDecimal => BigDecimalJ}
import java.util

import io.delta.kernel.data.{ColumnVector, VariantValue}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector
import io.delta.kernel.expressions.{Column, ToJson}
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class ToJsonExpressionEvaluatorSuite extends AnyFunSuite {
  test("serializes nulls, nested values, and primitive formatting with Rust semantics") {
    val ints = new ArrayType(IntegerType.INTEGER, true)
    val strings = new MapType(StringType.STRING, StringType.STRING, true)
    val nested = new StructType().add("z", IntegerType.INTEGER, true)
    val struct = new StructType()
      .add("missing", StringType.STRING, true)
      .add("b", BinaryType.BINARY, true)
      .add("l", ints, true)
      .add("n", nested, true)
      .add("m", strings, true)
      .add("d", DateType.DATE, true)
      .add("ts", TimestampType.TIMESTAMP, true)
      .add("ntz", TimestampNTZType.TIMESTAMP_NTZ, true)
      .add("f", FloatType.FLOAT, true)
      .add("g", DoubleType.DOUBLE, true)
      .add("decimal", new DecimalType(6, 2), true)

    val value = GenericRow.fromValues(
      struct,
      util.Arrays.asList(
        null,
        Array[Byte](0xAB.toByte, 0xCD.toByte),
        VectorUtils.buildArrayValue(
          util.Arrays.asList(IntegerJ.valueOf(1), null, IntegerJ.valueOf(2)),
          IntegerType.INTEGER),
        GenericRow.fromValues(nested, util.Arrays.asList(IntegerJ.valueOf(7))),
        VectorUtils.buildMapValue(
          util.Arrays.asList("x", "x", "skip"),
          util.Arrays.asList("first", "last", null),
          strings),
        IntegerJ.valueOf(0),
        LongJ.valueOf(0L),
        LongJ.valueOf(1234000L),
        FloatJ.valueOf(Float.NaN),
        DoubleJ.valueOf(Double.PositiveInfinity),
        new BigDecimalJ("12.30")))
    val input = batch("s", struct, value, null)

    val result = evaluate(input, new ToJson(new Column("s")))

    assert(result.isNullAt(1))
    assert(
      result.getString(0) ===
        """{"b":"abcd","l":[1,null,2],"n":{"z":7},""" +
        """"m":{"x":"first","x":"last"},"d":"1970-01-01",""" +
        """"ts":"1970-01-01T00:00:00Z",""" +
        """"ntz":"1970-01-01T00:00:01.234","f":null,"g":null,""" +
        """"decimal":12.30}""")
  }

  test("serializes intervals, variant, void, and finite floating point values") {
    val struct = new StructType()
      .add("year_month", IntervalYearMonthType.INTERVAL_YEAR_MONTH, false)
      .add("day_time", IntervalDayTimeType.INTERVAL_DAY_TIME, false)
      .add("variant", VariantType.VARIANT, false)
      .add("void", VoidType.VOID, true)
      .add("small", FloatType.FLOAT, false)
      .add("large", DoubleType.DOUBLE, false)
    val value = GenericRow.fromValues(
      struct,
      util.Arrays.asList(
        IntegerJ.valueOf(15),
        LongJ.valueOf(1234567L),
        new VariantValue(Array[Byte](1, 2), Array[Byte](10)),
        null,
        FloatJ.valueOf(1.0e-7f),
        DoubleJ.valueOf(1.0e20)))

    val result = evaluate(batch("s", struct, value), new ToJson(new Column("s")))

    assert(
      result.getString(0) ===
        """{"year_month":15,"day_time":1234567,""" +
        """"variant":{"metadata":"0a","value":"0102"},""" +
        """"small":1.0e-7,"large":1.0e20}""")
  }

  test("rejects non-struct input") {
    val error = intercept[UnsupportedOperationException] {
      new DefaultExpressionEvaluator(
        new StructType().add("value", IntegerType.INTEGER),
        new ToJson(new Column("value")),
        StringType.STRING)
    }
    assert(error.getMessage.contains("TO_JSON requires a struct input"))
  }

  private def batch(
      name: String,
      dataType: DataType,
      values: AnyRef*): DefaultColumnarBatch = {
    new DefaultColumnarBatch(
      values.size,
      new StructType().add(name, dataType),
      Array(DefaultGenericVector.fromArray(dataType, values.toArray)))
  }

  private def evaluate(
      input: DefaultColumnarBatch,
      expression: ToJson): ColumnVector = {
    new DefaultExpressionEvaluator(input.getSchema, expression, StringType.STRING).eval(input)
  }
}
