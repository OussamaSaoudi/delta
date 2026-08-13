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

import java.lang.{Integer => IntegerJ}

import scala.collection.JavaConverters._

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue, VariantValue}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector
import io.delta.kernel.expressions.{Column, Expression, Literal, ScalarExpression, StructExpression}
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class ArrayExpressionEvaluatorSuite extends AnyFunSuite with ExpressionSuiteBase {
  test("ARRAY constructs row-major primitive arrays and preserves null elements") {
    val input = batch(
      ("left", IntegerType.INTEGER, Seq(IntegerJ.valueOf(1), IntegerJ.valueOf(2))),
      ("right", IntegerType.INTEGER, Seq(null, IntegerJ.valueOf(20))))
    val outputType = new ArrayType(IntegerType.INTEGER, true)
    val result = evaluate(
      input,
      array(new Column("left"), new Column("right"), Literal.ofInt(42)),
      outputType)

    assert(result.getDataType === outputType)
    assert((0 until result.getSize).forall(rowId => !result.isNullAt(rowId)))
    assertInts(result.getArray(0), Seq(IntegerJ.valueOf(1), null, IntegerJ.valueOf(42)))
    assertInts(
      result.getArray(1),
      Seq(IntegerJ.valueOf(2), IntegerJ.valueOf(20), IntegerJ.valueOf(42)))
  }

  test("ARRAY propagates exact nested types through arithmetic, COALESCE, and struct") {
    val input = batch(
      ("left", IntegerType.INTEGER, Seq(IntegerJ.valueOf(1), IntegerJ.valueOf(2))),
      ("right", IntegerType.INTEGER, Seq(IntegerJ.valueOf(10), IntegerJ.valueOf(20))))
    val elementType = new StructType().add("id", IntegerType.INTEGER, false)
    val arrayType = new ArrayType(elementType, false)
    val leftStruct = new StructExpression(Seq[Expression](add(new Column("left"), 1)).asJava)
    val rightStruct = new StructExpression(Seq[Expression](new Column("right")).asJava)
    val constructed = array(leftStruct, rightStruct)
    val expression = new StructExpression(
      Seq[Expression](coalesce(constructed, new Column("missing"))).asJava)
    val outputType = new StructType().add("values", arrayType, false)

    val result = evaluate(input, expression, outputType).getChild(0)
    val rowZeroElements = result.getArray(0).getElements
    assert(rowZeroElements.getChild(0).getInt(0) === 2)
    assert(rowZeroElements.getChild(0).getInt(1) === 10)
    val rowOneElements = result.getArray(1).getElements
    assert(rowOneElements.getChild(0).getInt(0) === 3)
    assert(rowOneElements.getChild(0).getInt(1) === 20)
  }

  test("ARRAY exposes nested array and map elements without conversion") {
    val mapType = new MapType(StringType.STRING, IntegerType.INTEGER, false)
    val leftMap = mapValue(mapType, Seq("a"), Seq(IntegerJ.valueOf(1)))
    val rightMap = mapValue(mapType, Seq("b"), Seq(IntegerJ.valueOf(2)))
    val maps = batch(
      ("left", mapType, Seq(leftMap.asInstanceOf[AnyRef])),
      ("right", mapType, Seq(rightMap.asInstanceOf[AnyRef])))
    val mapResult = evaluate(
      maps,
      array(new Column("left"), new Column("right")),
      new ArrayType(mapType, false)).getArray(0).getElements
    assert(mapResult.getMap(0).getKeys.getString(0) === "a")
    assert(mapResult.getMap(1).getValues.getInt(0) === 2)

    val nested = evaluate(
      batch(("left", IntegerType.INTEGER, Seq(IntegerJ.valueOf(7)))),
      array(array(new Column("left"), Literal.ofInt(8)), array(Literal.ofInt(9))),
      new ArrayType(new ArrayType(IntegerType.INTEGER, false), false))
    val nestedElements = nested.getArray(0).getElements
    assertInts(nestedElements.getArray(0), Seq(IntegerJ.valueOf(7), IntegerJ.valueOf(8)))
    assertInts(nestedElements.getArray(1), Seq(IntegerJ.valueOf(9)))
  }

  test("ARRAY and COALESCE preserve Variant values without conversion") {
    val first = new VariantValue(Array[Byte](1), Array[Byte](10))
    val fallback = new VariantValue(Array[Byte](2), Array[Byte](20))
    val input = batch(
      ("left", VariantType.VARIANT, Seq(first, null)),
      ("right", VariantType.VARIANT, Seq(fallback, fallback)))

    val result = evaluate(
      input,
      array(coalesce(new Column("left"), new Column("right")), new Column("right")),
      new ArrayType(VariantType.VARIANT, false))

    val firstRow = result.getArray(0).getElements
    assert(firstRow.getVariant(0) === first)
    assert(firstRow.getVariant(1) === fallback)
    val secondRow = result.getArray(1).getElements
    assert(secondRow.getVariant(0) === fallback)
    assert(secondRow.getVariant(1) === fallback)
  }

  test("ARRAY supports zero rows and validates type, arity, and nullability") {
    val outputType = new ArrayType(IntegerType.INTEGER, false)
    val emptyResult = evaluate(
      zeroColumnBatch(0),
      array(Literal.ofInt(1), Literal.ofInt(2)),
      outputType)
    assert(emptyResult.getSize === 0)
    assert(emptyResult.getDataType === outputType)

    assert(intercept[UnsupportedOperationException] {
      new DefaultExpressionEvaluator(new StructType(), array(), outputType)
    }.getMessage.contains("requires at least one element"))
    assert(intercept[UnsupportedOperationException] {
      new DefaultExpressionEvaluator(
        new StructType(),
        array(Literal.ofInt(1), Literal.ofLong(2L)),
        new ArrayType(IntegerType.INTEGER, true))
    }.getMessage.contains("inputs must share the same element type"))
    assert(intercept[UnsupportedOperationException] {
      new DefaultExpressionEvaluator(new StructType(), array(Literal.ofInt(1)), LongType.LONG)
    }.getMessage.contains("requires an ArrayType result type"))

    val nullableInput = batch(
      ("value", IntegerType.INTEGER, Seq(IntegerJ.valueOf(1), null)))
    val error = intercept[IllegalArgumentException] {
      evaluate(nullableInput, array(new Column("value")), outputType)
    }
    assert(error.getMessage.contains("declares non-nullable elements"))
  }

  test("ARRAY owns evaluated children once and never closes borrowed input vectors") {
    val first = new TrackingIntVector(Seq(IntegerJ.valueOf(1)))
    val second = new TrackingIntVector(Seq(IntegerJ.valueOf(2)))
    val expression = array(Literal.ofInt(1), Literal.ofInt(2))
    val result = ArrayExpressionEvaluator.eval(
      expression,
      Seq[ColumnVector](first, second).asJava,
      new ArrayType(IntegerType.INTEGER, false),
      1)

    result.close()
    result.close()
    assert(first.closeCount === 1)
    assert(second.closeCount === 1)

    val borrowed = new TrackingIntVector(Seq(IntegerJ.valueOf(3)))
    val input = new DefaultColumnarBatch(
      1,
      new StructType().add("value", IntegerType.INTEGER),
      Array[ColumnVector](borrowed))
    evaluate(
      input,
      array(new Column("value")),
      new ArrayType(IntegerType.INTEGER, false)).close()
    assert(borrowed.closeCount === 0)
  }

  private def array(children: Expression*): ScalarExpression = {
    new ScalarExpression("ARRAY", children.toList.asJava)
  }

  private def coalesce(children: Expression*): ScalarExpression = {
    new ScalarExpression("COALESCE", children.toList.asJava)
  }

  private def add(left: Expression, value: Int): ScalarExpression = {
    new ScalarExpression("ADD", Seq[Expression](left, Literal.ofInt(value)).asJava)
  }

  private def evaluate(
      input: io.delta.kernel.data.ColumnarBatch,
      expression: Expression,
      outputType: DataType): ColumnVector = {
    new DefaultExpressionEvaluator(input.getSchema, expression, outputType).eval(input)
  }

  private def batch(columns: (String, DataType, Seq[AnyRef])*): DefaultColumnarBatch = {
    val schema = columns.foldLeft(new StructType()) {
      case (result, (name, dataType, _)) => result.add(name, dataType)
    }
    val vectors = columns.map { case (_, dataType, values) =>
      DefaultGenericVector.fromArray(dataType, values.toArray)
    }
    new DefaultColumnarBatch(columns.head._3.size, schema, vectors.toArray)
  }

  private def mapValue(
      dataType: MapType,
      keys: Seq[AnyRef],
      values: Seq[AnyRef]): MapValue = new MapValue {
    override def getSize: Int = keys.size
    override def getKeys: ColumnVector = DefaultGenericVector.fromArray(
      dataType.getKeyType,
      keys.toArray)
    override def getValues: ColumnVector = DefaultGenericVector.fromArray(
      dataType.getValueType,
      values.toArray)
  }

  private def assertInts(value: ArrayValue, expected: Seq[IntegerJ]): Unit = {
    val elements = value.getElements
    assert(elements.getDataType === IntegerType.INTEGER)
    assert(elements.getSize === expected.size)
    expected.indices.foreach { index =>
      assert(elements.isNullAt(index) === (expected(index) == null))
      if (expected(index) != null) {
        assert(elements.getInt(index) === expected(index).intValue())
      }
    }
  }

  private class TrackingIntVector(values: Seq[IntegerJ]) extends ColumnVector {
    var closeCount = 0

    override def getDataType: DataType = IntegerType.INTEGER
    override def getSize: Int = values.size
    override def close(): Unit = closeCount += 1
    override def isNullAt(rowId: Int): Boolean = values(rowId) == null
    override def getInt(rowId: Int): Int = values(rowId).intValue()
  }
}
