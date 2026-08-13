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
import java.math.{BigDecimal => BigDecimalJ}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ArrayValue, ColumnarBatch, ColumnVector, MapValue}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector
import io.delta.kernel.expressions._
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class PlanPredicateEvaluatorSuite extends AnyFunSuite {
  private def vector(dataType: DataType, values: Seq[AnyRef]): ColumnVector = {
    DefaultGenericVector.fromArray(dataType, values.toArray)
  }

  private def batch(
      schema: StructType,
      size: Int,
      columns: ColumnVector*): ColumnarBatch = {
    new DefaultColumnarBatch(size, schema, columns.toArray)
  }

  private def evaluate(
      input: ColumnarBatch,
      expression: Expression,
      outputType: DataType = BooleanType.BOOLEAN): ColumnVector = {
    new DefaultExpressionEvaluator(input.getSchema, expression, outputType).eval(input)
  }

  private def bools(vector: ColumnVector): Seq[java.lang.Boolean] = {
    (0 until vector.getSize).map { rowId =>
      if (vector.isNullAt(rowId)) null else BooleanJ.valueOf(vector.getBoolean(rowId))
    }
  }

  test("strict comparisons preserve null semantics and exact types") {
    val schema = new StructType()
      .add("left", IntegerType.INTEGER, true)
      .add("right", IntegerType.INTEGER, true)
    val input = batch(
      schema,
      4,
      vector(IntegerType.INTEGER, Seq(Int.box(1), null, Int.box(2), null)),
      vector(IntegerType.INTEGER, Seq(Int.box(2), Int.box(1), Int.box(2), null)))
    val cases = Seq(
      BinaryPredicate.Operator.LESS_THAN -> Seq(BooleanJ.TRUE, null, BooleanJ.FALSE, null),
      BinaryPredicate.Operator.GREATER_THAN -> Seq(BooleanJ.FALSE, null, BooleanJ.FALSE, null),
      BinaryPredicate.Operator.EQUAL -> Seq(BooleanJ.FALSE, null, BooleanJ.TRUE, null),
      BinaryPredicate.Operator.DISTINCT -> Seq(
        BooleanJ.TRUE,
        BooleanJ.TRUE,
        BooleanJ.FALSE,
        BooleanJ.FALSE))

    cases.foreach { case (operator, expected) =>
      val predicate = new BinaryPredicate(operator, new Column("left"), new Column("right"))
      val result = evaluate(input, predicate)
      assert(bools(result) === expected)
      result.close()
    }

    val mismatch = new BinaryPredicate(
      BinaryPredicate.Operator.EQUAL,
      Literal.ofInt(1),
      Literal.ofLong(1L))
    val error = intercept[UnsupportedOperationException] {
      new DefaultExpressionEvaluator(new StructType(), mismatch, BooleanType.BOOLEAN)
    }
    assert(error.getMessage.contains("requires exact type"))
  }

  test("geospatial comparisons use their Kernel string representation") {
    val geometryType = GeometryType.ofDefault()
    val geographyType = GeographyType.ofDefault()
    val schema = new StructType()
      .add("geometry", geometryType, false)
      .add("geography", geographyType, false)
    val input = batch(
      schema,
      1,
      vector(geometryType, Seq("POINT (0 0)")),
      vector(geographyType, Seq("POINT (1 1)")))
    val predicates = Seq(
      new BinaryPredicate(
        BinaryPredicate.Operator.EQUAL,
        new Column("geometry"),
        Literal.ofGeospatialWKT("POINT (0 0)", geometryType)),
      new BinaryPredicate(
        BinaryPredicate.Operator.LESS_THAN,
        new Column("geography"),
        Literal.ofGeospatialWKT("POINT (2 2)", geographyType)))

    predicates.foreach { predicate =>
      val result = evaluate(input, predicate)
      assert(result.getBoolean(0))
      result.close()
    }
  }

  test("junctions implement n-ary Kleene logic and empty identities") {
    val schema = new StructType()
      .add("left", BooleanType.BOOLEAN, true)
      .add("right", BooleanType.BOOLEAN, true)
    val input = batch(
      schema,
      3,
      vector(BooleanType.BOOLEAN, Seq(BooleanJ.FALSE, BooleanJ.TRUE, null)),
      vector(BooleanType.BOOLEAN, Seq(null, null, null)))
    val left = new BooleanExpression(new Column("left"))
    val right = new BooleanExpression(new Column("right"))
    val cases = Seq(
      new Junction(Junction.Operator.AND, Seq[Predicate](left, right).asJava) ->
        Seq(BooleanJ.FALSE, null, null),
      new Junction(Junction.Operator.OR, Seq[Predicate](left, right).asJava) ->
        Seq(null, BooleanJ.TRUE, null),
      new Junction(Junction.Operator.AND, Seq.empty[Predicate].asJava) ->
        Seq.fill(3)(BooleanJ.TRUE),
      new Junction(Junction.Operator.OR, Seq.empty[Predicate].asJava) ->
        Seq.fill(3)(BooleanJ.FALSE))

    cases.foreach { case (predicate, expected) =>
      assert(bools(evaluate(input, predicate)) === expected)
    }
    assert(bools(evaluate(input, left)) === Seq(BooleanJ.FALSE, BooleanJ.TRUE, null))

    val invalid = new BooleanExpression(new Column("number"))
    val error = intercept[UnsupportedOperationException] {
      new DefaultExpressionEvaluator(
        new StructType().add("number", IntegerType.INTEGER),
        invalid,
        BooleanType.BOOLEAN)
    }
    assert(error.getMessage.contains("requires exact type"))
  }

  test("predicate result vectors close their inputs exactly once") {
    val leftCloses = new AtomicInteger()
    val rightCloses = new AtomicInteger()
    val left = new TrackingVector(vector(IntegerType.INTEGER, Seq(Int.box(1))), leftCloses)
    val right = new TrackingVector(vector(IntegerType.INTEGER, Seq(Int.box(1))), rightCloses)
    val comparison = PlanPredicateEvaluator.strictComparison(
      BinaryPredicate.Operator.EQUAL,
      left,
      right)
    assert(comparison.getBoolean(0))
    comparison.close()
    comparison.close()
    assert(leftCloses.get() === 1)
    assert(rightCloses.get() === 1)

    val booleanCloses = new AtomicInteger()
    val booleanInput = new TrackingVector(
      vector(BooleanType.BOOLEAN, Seq(BooleanJ.TRUE)),
      booleanCloses)
    val wrapped = DefaultExpressionUtils.booleanWrapperVector(
      booleanInput,
      rowId => booleanInput.getBoolean(rowId),
      rowId => booleanInput.isNullAt(rowId))
    wrapped.close()
    wrapped.close()
    assert(booleanCloses.get() === 1)
  }

  private class TrackingVector(delegate: ColumnVector, closes: AtomicInteger)
      extends ColumnVector {
    override def getDataType: DataType = delegate.getDataType
    override def getSize: Int = delegate.getSize
    override def close(): Unit = {
      closes.incrementAndGet()
      delegate.close()
    }
    override def isNullAt(rowId: Int): Boolean = delegate.isNullAt(rowId)
    override def getBoolean(rowId: Int): Boolean = delegate.getBoolean(rowId)
    override def getByte(rowId: Int): Byte = delegate.getByte(rowId)
    override def getShort(rowId: Int): Short = delegate.getShort(rowId)
    override def getInt(rowId: Int): Int = delegate.getInt(rowId)
    override def getLong(rowId: Int): Long = delegate.getLong(rowId)
    override def getFloat(rowId: Int): Float = delegate.getFloat(rowId)
    override def getDouble(rowId: Int): Double = delegate.getDouble(rowId)
    override def getBinary(rowId: Int): Array[Byte] = delegate.getBinary(rowId)
    override def getString(rowId: Int): String = delegate.getString(rowId)
    override def getDecimal(rowId: Int): BigDecimalJ = delegate.getDecimal(rowId)
    override def getMap(rowId: Int): MapValue = delegate.getMap(rowId)
    override def getArray(rowId: Int): ArrayValue = delegate.getArray(rowId)
    override def getChild(ordinal: Int): ColumnVector = delegate.getChild(ordinal)
  }
}
