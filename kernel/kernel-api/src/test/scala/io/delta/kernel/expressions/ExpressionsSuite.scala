/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.expressions

import java.util.{Arrays, Collections}

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue, Row}
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class ExpressionsSuite extends AnyFunSuite {
  test("nested null literals preserve their declared type") {
    Seq[DataType](
      new ArrayType(IntegerType.INTEGER, true),
      new MapType(IntegerType.INTEGER, IntegerType.INTEGER, true),
      new StructType().add("s1", BooleanType.BOOLEAN)).foreach { dataType =>
      val literal = Literal.ofNull(dataType)
      assert(literal.getValue == null)
      assert(literal.getDataType == dataType)
    }
  }

  test("complex literal factories preserve Kernel data values") {
    val arrayType = new ArrayType(IntegerType.INTEGER, false)
    val array = VectorUtils.buildArrayValue(Arrays.asList(1, 2), IntegerType.INTEGER)
    val arrayLiteral = Literal.ofArray(array, arrayType)
    assert(arrayLiteral.getValue.asInstanceOf[ArrayValue].getElements.getInt(1) == 2)

    val mapType = new MapType(StringType.STRING, IntegerType.INTEGER, false)
    val map = VectorUtils.buildMapValue(
      Arrays.asList("one", "two"),
      Arrays.asList(1, 2),
      mapType)
    val mapLiteral = Literal.ofMap(map, mapType)
    assert(mapLiteral.getValue.asInstanceOf[MapValue].getValues.getInt(0) == 1)

    val structType = new StructType().add("id", IntegerType.INTEGER, false)
    val row = GenericRow.fromValues(structType, Arrays.asList[AnyRef](Integer.valueOf(7)))
    val structLiteral = Literal.ofStruct(row, structType)
    assert(structLiteral.getValue.asInstanceOf[Row].getInt(0) == 7)
  }

  private val intArray = new ArrayType(IntegerType.INTEGER, false)
  private val stringIntMap = new MapType(StringType.STRING, IntegerType.INTEGER, false)
  private val requiredIntStruct = new StructType().add("id", IntegerType.INTEGER, false)

  private def arrayValue(size: Int, elements: ColumnVector): ArrayValue = new ArrayValue {
    override def getSize: Int = size
    override def getElements: ColumnVector = elements
  }

  private def mapValue(size: Int, keys: ColumnVector, values: ColumnVector): MapValue =
    new MapValue {
      override def getSize: Int = size
      override def getKeys: ColumnVector = keys
      override def getValues: ColumnVector = values
    }

  private val oneInt = VectorUtils.buildColumnVector(Arrays.asList(1), IntegerType.INTEGER)
  private val twoInts = VectorUtils.buildColumnVector(Arrays.asList(1, 2), IntegerType.INTEGER)
  private val oneString = VectorUtils.buildColumnVector(Arrays.asList("a"), StringType.STRING)
  private val twoStrings = VectorUtils.buildColumnVector(
    Arrays.asList("a", "b"),
    StringType.STRING)
  private val nullableInt = VectorUtils.buildColumnVector(
    Arrays.asList[Integer](null),
    IntegerType.INTEGER)
  private val nullableString = VectorUtils.buildColumnVector(
    Arrays.asList[String](null),
    StringType.STRING)

  Seq[(String, () => Unit)](
    "negative array size" -> (() => Literal.ofArray(arrayValue(-1, oneInt), intArray)),
    "array element count" -> (() => Literal.ofArray(arrayValue(2, oneInt), intArray)),
    "array element type" -> (() => Literal.ofArray(arrayValue(1, oneString), intArray)),
    "array element nullability" ->
      (() => Literal.ofArray(arrayValue(1, nullableInt), intArray)),
    "negative map size" ->
      (() => Literal.ofMap(mapValue(-1, oneString, oneInt), stringIntMap)),
    "map key count" ->
      (() => Literal.ofMap(mapValue(2, oneString, twoInts), stringIntMap)),
    "map value count" ->
      (() => Literal.ofMap(mapValue(2, twoStrings, oneInt), stringIntMap)),
    "map key type" ->
      (() => Literal.ofMap(mapValue(1, oneInt, oneInt), stringIntMap)),
    "map value type" ->
      (() => Literal.ofMap(mapValue(1, oneString, oneString), stringIntMap)),
    "map null key" ->
      (() => Literal.ofMap(mapValue(1, nullableString, oneInt), stringIntMap)),
    "map value nullability" ->
      (() => Literal.ofMap(mapValue(1, oneString, nullableInt), stringIntMap)),
    "map builder entry count" ->
      (() => VectorUtils.buildMapValue(Arrays.asList("a"), Arrays.asList(1, 2), stringIntMap)),
    "struct schema" -> (() =>
      Literal.ofStruct(
        GenericRow.fromValues(
          new StructType().add("other", IntegerType.INTEGER, false),
          Collections.singletonList(Integer.valueOf(1))),
        requiredIntStruct)),
    "struct field nullability" -> (() =>
      Literal.ofStruct(
        GenericRow.fromValues(requiredIntStruct, Collections.singletonList[AnyRef](null)),
        requiredIntStruct))).foreach { case (name, invalidLiteral) =>
    test(s"complex literal validation: $name") {
      intercept[IllegalArgumentException](invalidLiteral())
    }
  }

  test("ofDecimal: adjusts precision when scale exceeds caller-provided precision") {
    // Java's BigDecimal.precision() returns the count of significant digits in the
    // unscaled value, not the SQL precision. For example, BigDecimal.valueOf(0, 18) has
    // precision=1 and scale=18. A naive caller passing bd.precision() as the precision
    // argument would create DecimalType(1, 18) which is invalid. ofDecimal should
    // adjust precision upward to at least scale.
    val bd = java.math.BigDecimal.valueOf(0, 18)
    assert(bd.precision() == 1)
    assert(bd.scale() == 18)
    val lit = Literal.ofDecimal(bd, bd.precision(), bd.scale())
    val dt = lit.getDataType.asInstanceOf[DecimalType]
    assert(dt.getPrecision == 18)
    assert(dt.getScale == 18)
  }

  test("ofDecimal: normal case with precision >= scale is unchanged") {
    val bd = new java.math.BigDecimal("123.45")
    val lit = Literal.ofDecimal(bd, 10, 2)
    val dt = lit.getDataType.asInstanceOf[DecimalType]
    assert(dt.getPrecision == 10)
    assert(dt.getScale == 2)
    assert(lit.getValue.asInstanceOf[java.math.BigDecimal].compareTo(bd) == 0)
  }

  test("ofDecimal: rejects scale exceeding DecimalType max precision (38)") {
    val bd = java.math.BigDecimal.valueOf(0, 39) // scale=39 > MAX_PRECISION
    val ex = intercept[IllegalArgumentException] {
      Literal.ofDecimal(bd, bd.precision(), bd.scale())
    }
    assert(ex.getMessage.contains("Invalid precision and scale combo"))
  }

  test("ofDecimal: rejects value that exceeds adjusted precision") {
    // BigDecimal "99999.99" has 7 significant digits, so precision=5, scale=2 is too small
    val bd = new java.math.BigDecimal("99999.99")
    val ex = intercept[IllegalArgumentException] {
      Literal.ofDecimal(bd, 5, 2)
    }
    assert(ex.getMessage.contains("exceeds max precision"))
  }
}
