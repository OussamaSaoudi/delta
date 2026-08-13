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

import java.lang.{Boolean => BooleanJ}
import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ArrayValue, ColumnarBatch, ColumnVector, MapValue, Row}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector
import io.delta.kernel.expressions._
import io.delta.kernel.internal.data.{GenericColumnVector, GenericRow}
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class PlanInPredicateEvaluatorSuite extends AnyFunSuite {
  private def vector(dataType: DataType, values: Seq[AnyRef]): ColumnVector = {
    DefaultGenericVector.fromArray(dataType, values.toArray)
  }

  private def arrayValue(dataType: ArrayType, values: Seq[AnyRef]): ArrayValue = {
    val elements = new GenericColumnVector(values.asJava, dataType.getElementType)
    new ArrayValue {
      override def getSize: Int = elements.getSize
      override def getElements: ColumnVector = elements
    }
  }

  private def mapValue(
      dataType: MapType,
      keys: Seq[AnyRef],
      values: Seq[AnyRef]): MapValue = {
    val keyVector = new GenericColumnVector(keys.asJava, dataType.getKeyType)
    val valueVector = new GenericColumnVector(values.asJava, dataType.getValueType)
    new MapValue {
      override def getSize: Int = keyVector.getSize
      override def getKeys: ColumnVector = keyVector
      override def getValues: ColumnVector = valueVector
    }
  }

  private def row(schema: StructType, values: Seq[AnyRef]): Row = {
    val byOrdinal = new util.HashMap[Integer, Object]()
    values.zipWithIndex.foreach { case (value, ordinal) => byOrdinal.put(ordinal, value) }
    new GenericRow(schema, byOrdinal)
  }

  private def batch(
      schema: StructType,
      size: Int,
      columns: ColumnVector*): ColumnarBatch = {
    new DefaultColumnarBatch(size, schema, columns.toArray)
  }

  private def evaluate(input: ColumnarBatch, expression: Expression): ColumnVector = {
    new DefaultExpressionEvaluator(input.getSchema, expression, BooleanType.BOOLEAN).eval(input)
  }

  private def bools(vector: ColumnVector): Seq[java.lang.Boolean] = {
    (0 until vector.getSize).map { rowId =>
      if (vector.isNullAt(rowId)) null else BooleanJ.valueOf(vector.getBoolean(rowId))
    }
  }

  test("plan IN accepts Rust operand shapes and never returns null") {
    val arrayType = new ArrayType(IntegerType.INTEGER, true)
    val oneNullTwo = arrayValue(arrayType, Seq(Int.box(1), null, Int.box(2)))
    val three = arrayValue(arrayType, Seq(Int.box(3)))
    val schema = new StructType().add("values", arrayType, true)
    val input = batch(
      schema,
      3,
      vector(arrayType, Seq(oneNullTwo.asInstanceOf[AnyRef], three.asInstanceOf[AnyRef], null)))

    val present = new BinaryPredicate(
      BinaryPredicate.Operator.IN,
      Literal.ofInt(2),
      new Column("values"))
    val nullNeedle = new BinaryPredicate(
      BinaryPredicate.Operator.IN,
      Literal.ofNull(IntegerType.INTEGER),
      new Column("values"))
    assert(bools(evaluate(input, present)) === Seq(BooleanJ.TRUE, BooleanJ.FALSE, BooleanJ.FALSE))
    assert(bools(evaluate(input, nullNeedle)) === Seq.fill(3)(BooleanJ.FALSE))

    val literalElements = Literal.ofArray(oneNullTwo, arrayType)
    Seq(
      Literal.ofInt(2) -> true,
      Literal.ofNull(IntegerType.INTEGER) -> true,
      Literal.ofInt(9) -> false).foreach { case (needle, expected) =>
      val predicate = new BinaryPredicate(BinaryPredicate.Operator.IN, needle, literalElements)
      assert(evaluate(batch(new StructType(), 1), predicate).getBoolean(0) === expected)
    }

    val closes = new AtomicInteger()
    val trackedLists = new TrackingArrayVector(
      vector(arrayType, Seq(oneNullTwo.asInstanceOf[AnyRef])),
      closes)
    val direct = PlanPredicateEvaluator.inList(Literal.ofInt(2), trackedLists, false)
    assert(direct.getBoolean(0))
    assert(closes.get() === 1)
  }

  test("plan IN rejects unsupported shapes, types, and column elements") {
    val intArrayType = new ArrayType(IntegerType.INTEGER, true)
    val intElements = Literal.ofArray(arrayValue(intArrayType, Seq(Int.box(1))), intArrayType)
    val columnNeedle = new BinaryPredicate(
      BinaryPredicate.Operator.IN,
      new Column("needle"),
      intElements)
    val columnError = intercept[UnsupportedOperationException] {
      new DefaultExpressionEvaluator(
        new StructType().add("needle", IntegerType.INTEGER),
        columnNeedle,
        BooleanType.BOOLEAN)
    }
    assert(columnError.getMessage.contains("literal left operand"))

    val typeMismatch = new BinaryPredicate(
      BinaryPredicate.Operator.IN,
      Literal.ofLong(1L),
      intElements)
    val mismatchError = intercept[UnsupportedOperationException] {
      new DefaultExpressionEvaluator(new StructType(), typeMismatch, BooleanType.BOOLEAN)
    }
    assert(mismatchError.getMessage.contains("requires exact type"))

    val nestedArrayType = new ArrayType(IntegerType.INTEGER, true)
    val nestedMapType = new MapType(StringType.STRING, IntegerType.INTEGER, true)
    val nestedStructType = new StructType().add("x", IntegerType.INTEGER, true)
    val unsupported = Seq[(Literal, DataType)](
      Literal.ofBoolean(true) -> BooleanType.BOOLEAN,
      Literal.ofBinary(Array[Byte](1)) -> BinaryType.BINARY,
      Literal.ofArray(arrayValue(nestedArrayType, Seq(Int.box(1))), nestedArrayType) ->
        nestedArrayType,
      Literal.ofMap(mapValue(nestedMapType, Seq("k"), Seq(Int.box(1))), nestedMapType) ->
        nestedMapType,
      Literal.ofStruct(row(nestedStructType, Seq(Int.box(1))), nestedStructType) ->
        nestedStructType)
    unsupported.foreach { case (needle, elementType) =>
      val schema = new StructType().add("values", new ArrayType(elementType, true), true)
      val predicate = new BinaryPredicate(
        BinaryPredicate.Operator.IN,
        needle,
        new Column("values"))
      val error = intercept[UnsupportedOperationException] {
        new DefaultExpressionEvaluator(schema, predicate, BooleanType.BOOLEAN)
      }
      assert(error.getMessage.contains("does not support array column elements"))
    }
  }

  test("literal-array plan IN compares complex values recursively") {
    val tagsType = new ArrayType(StringType.STRING, true)
    val attributesType = new MapType(StringType.STRING, BinaryType.BINARY, true)
    val structType = new StructType()
      .add("id", IntegerType.INTEGER, false)
      .add("tags", tagsType, true)
      .add("attributes", attributesType, true)
    val tags = arrayValue(tagsType, Seq("a", null, "c"))
    val attributes = mapValue(
      attributesType,
      Seq("first", "second"),
      Seq(Array[Byte](1, 2), null))
    val needle = row(
      structType,
      Seq(Int.box(7), tags.asInstanceOf[AnyRef], attributes.asInstanceOf[AnyRef]))
    val candidate = row(
      structType,
      Seq(Int.box(7), tags.asInstanceOf[AnyRef], attributes.asInstanceOf[AnyRef]))
    val candidatesType = new ArrayType(structType, false)
    val candidates = arrayValue(candidatesType, Seq(candidate.asInstanceOf[AnyRef]))
    val predicate = new BinaryPredicate(
      BinaryPredicate.Operator.IN,
      Literal.ofStruct(needle, structType),
      Literal.ofArray(candidates, candidatesType))

    assert(evaluate(batch(new StructType(), 1), predicate).getBoolean(0))
  }

  private class TrackingArrayVector(delegate: ColumnVector, closes: AtomicInteger)
      extends ColumnVector {
    override def getDataType: DataType = delegate.getDataType
    override def getSize: Int = delegate.getSize
    override def close(): Unit = {
      closes.incrementAndGet()
      delegate.close()
    }
    override def isNullAt(rowId: Int): Boolean = delegate.isNullAt(rowId)
    override def getArray(rowId: Int): ArrayValue = delegate.getArray(rowId)
  }
}
