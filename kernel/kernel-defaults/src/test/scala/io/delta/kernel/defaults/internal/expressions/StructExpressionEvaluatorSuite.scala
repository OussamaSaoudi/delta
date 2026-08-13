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

import java.util

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ColumnarBatch, ColumnVector}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.{DefaultGenericVector, DefaultStructVector}
import io.delta.kernel.expressions._
import io.delta.kernel.internal.data.StructRow
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class StructExpressionEvaluatorSuite extends AnyFunSuite {
  private def vector(dataType: DataType, values: AnyRef*): ColumnVector =
    DefaultGenericVector.fromArray(dataType, values.toArray)

  private def batch(fields: (StructField, ColumnVector)*): ColumnarBatch = {
    val rowCount = fields.headOption.map(_._2.getSize).getOrElse(0)
    new DefaultColumnarBatch(
      rowCount,
      new StructType(fields.map(_._1).asJava),
      fields.map(_._2).toArray)
  }

  private def evaluate(
      input: ColumnarBatch,
      expression: Expression,
      outputType: DataType): ColumnVector =
    new DefaultExpressionEvaluator(input.getSchema, expression, outputType).eval(input)

  test("struct fields use caller schema and remain Kernel-native") {
    val idVector = vector(IntegerType.INTEGER, Integer.valueOf(1), Integer.valueOf(2))
    val nameVector = vector(StringType.STRING, "one", "two")
    val input = batch(
      new StructField("id", IntegerType.INTEGER, false) -> idVector,
      new StructField("name", StringType.STRING, false) -> nameVector)
    val outputType = new StructType()
      .add("renamed_id", IntegerType.INTEGER, false)
      .add("renamed_name", StringType.STRING, true)
    val expression = new StructExpression(
      util.Arrays.asList(new Column("id"), new Column("name")))

    val result = evaluate(input, expression, outputType)
    assert(result.getDataType == outputType)
    assert(!result.isNullAt(0))
    val row = StructRow.fromStructVector(result, 1)
    assert(row.getSchema == outputType)
    assert(row.getInt(0) == 2)
    assert(row.getString(1) == "two")

    result.close()
    result.close()
    assert(idVector.getInt(0) == 1)
    assert(nameVector.getString(0) == "one")
  }

  test("false and null predicates make the whole struct null") {
    val input = batch(
      new StructField("value", IntegerType.INTEGER, true) ->
        vector(IntegerType.INTEGER, Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(3)),
      new StructField("keep", BooleanType.BOOLEAN, true) ->
        vector(BooleanType.BOOLEAN, java.lang.Boolean.TRUE, java.lang.Boolean.FALSE, null))
    val outputType = new StructType().add("value", IntegerType.INTEGER, true)
    val expression = new StructExpression(
      util.Collections.singletonList(new Column("value")),
      new Column("keep"))

    val result = evaluate(input, expression, outputType)
    assert(!result.isNullAt(0))
    assert(result.isNullAt(1))
    assert(result.isNullAt(2))
    assert(StructRow.fromStructVector(result, 1) == null)
    assert(StructRow.fromStructVector(result, 2) == null)
    result.close()
  }

  test("parent null mask permits nulls in non-nullable fields") {
    val input = batch(
      new StructField("value", IntegerType.INTEGER, true) ->
        vector(IntegerType.INTEGER, null, Integer.valueOf(2)),
      new StructField("keep", BooleanType.BOOLEAN, false) ->
        vector(BooleanType.BOOLEAN, java.lang.Boolean.FALSE, java.lang.Boolean.TRUE))
    val outputType = new StructType().add("value", IntegerType.INTEGER, false)
    val expression = new StructExpression(
      util.Collections.singletonList(new Column("value")),
      new Column("keep"))

    val result = evaluate(input, expression, outputType)
    assert(result.isNullAt(0))
    assert(!result.isNullAt(1))
    assert(result.getChild(0).getInt(1) == 2)
    result.close()
  }

  Seq[(String, Option[Expression])](
    "without predicate" -> None,
    "with true predicate" -> Some(Literal.ofBoolean(true))).foreach { case (name, predicate) =>
    test(s"unmasked null in non-nullable field fails: $name") {
      val input = batch(
        new StructField("value", IntegerType.INTEGER, true) ->
          vector(IntegerType.INTEGER, null))
      val outputType = new StructType().add("value", IntegerType.INTEGER, false)
      val fields = util.Collections.singletonList[Expression](new Column("value"))
      val expression = predicate
        .map(value => new StructExpression(fields, value))
        .getOrElse(new StructExpression(fields))

      intercept[IllegalArgumentException] {
        evaluate(input, expression, outputType)
      }
    }
  }

  test("nested struct expressions receive their field schema") {
    val input = batch(
      new StructField("id", IntegerType.INTEGER, false) ->
        vector(IntegerType.INTEGER, Integer.valueOf(1), Integer.valueOf(2)),
      new StructField("text", StringType.STRING, false) -> vector(StringType.STRING, "a", "b"),
      new StructField("inner_keep", BooleanType.BOOLEAN, false) ->
        vector(BooleanType.BOOLEAN, java.lang.Boolean.TRUE, java.lang.Boolean.FALSE))
    val innerType = new StructType()
      .add("inner_id", IntegerType.INTEGER, false)
      .add("inner_text", StringType.STRING, true)
    val outputType = new StructType()
      .add("outer_id", IntegerType.INTEGER, false)
      .add("nested", innerType, true)
    val innerExpression = new StructExpression(
      util.Arrays.asList(new Column("id"), new Column("text")),
      new Column("inner_keep"))
    val expression = new StructExpression(
      util.Arrays.asList(new Column("id"), innerExpression))

    val result = evaluate(input, expression, outputType)
    val first = StructRow.fromStructVector(result, 0)
    assert(first.getInt(0) == 1)
    assert(first.getStruct(1).getInt(0) == 1)
    assert(first.getStruct(1).getString(1) == "a")
    assert(StructRow.fromStructVector(result, 1).getStruct(1) == null)
    result.close()
    result.close()
  }

  test("nested struct field nullability is matched exactly") {
    val actualNestedType = new StructType().add("value", IntegerType.INTEGER, true)
    val expectedNestedType = new StructType().add("value", IntegerType.INTEGER, false)
    val nestedVector = new DefaultStructVector(
      1,
      actualNestedType,
      util.Optional.empty(),
      Array(vector(IntegerType.INTEGER, Integer.valueOf(1))))
    val input = batch(
      new StructField("nested", actualNestedType, true) -> nestedVector)
    val outputType = new StructType().add("nested", expectedNestedType, true)
    val expression = new StructExpression(
      util.Collections.singletonList(new Column("nested")))

    intercept[UnsupportedOperationException] {
      new DefaultExpressionEvaluator(input.getSchema, expression, outputType)
    }
  }

  Seq[(String, StructExpression, DataType)](
    (
      "non-struct output",
      new StructExpression(util.Collections.singletonList(Literal.ofInt(1))),
      IntegerType.INTEGER),
    (
      "too few fields",
      new StructExpression(util.Collections.emptyList()),
      new StructType().add("value", IntegerType.INTEGER)),
    (
      "field type mismatch",
      new StructExpression(util.Collections.singletonList(Literal.ofString("bad"))),
      new StructType().add("value", IntegerType.INTEGER)),
    (
      "non-boolean predicate",
      new StructExpression(
        util.Collections.singletonList(Literal.ofInt(1)),
        Literal.ofInt(1)),
      new StructType().add("value", IntegerType.INTEGER))).foreach {
    case (name, expression, outputType) =>
      test(s"invalid struct expression contract: $name") {
        intercept[UnsupportedOperationException] {
          new DefaultExpressionEvaluator(new StructType(), expression, outputType)
        }
      }
  }

  test("owned field and predicate vectors close exactly once") {
    val fieldExpression = Literal.ofInt(1)
    val predicateExpression = Literal.ofBoolean(true)
    val fieldVector = new TrackingVector(vector(IntegerType.INTEGER, Integer.valueOf(1)))
    val predicateVector = new TrackingVector(
      vector(BooleanType.BOOLEAN, java.lang.Boolean.TRUE))
    val expression = new StructExpression(
      util.Collections.singletonList(fieldExpression),
      predicateExpression)
    val outputType = new StructType().add("value", IntegerType.INTEGER, false)

    val result = StructExpressionEvaluator.eval(
      expression,
      outputType,
      1,
      (child, _) => if (child eq fieldExpression) fieldVector else predicateVector)
    result.close()
    result.close()
    assert(fieldVector.closeCount == 1)
    assert(predicateVector.closeCount == 1)
  }

  Seq[(String, TrackingVector)](
    "type" -> new TrackingVector(vector(StringType.STRING, "bad")),
    "size" -> new TrackingVector(
      vector(IntegerType.INTEGER, Integer.valueOf(1), Integer.valueOf(2)))).foreach {
    case (name, fieldVector) =>
      test(s"failed child $name validation closes evaluated vectors") {
        val fieldExpression = Literal.ofInt(1)
        val expression = new StructExpression(util.Collections.singletonList(fieldExpression))
        val outputType = new StructType().add("value", IntegerType.INTEGER, true)
        intercept[IllegalArgumentException] {
          StructExpressionEvaluator.eval(expression, outputType, 1, (_, _) => fieldVector)
        }
        assert(fieldVector.closeCount == 1)
      }
  }

  private class TrackingVector(delegate: ColumnVector) extends ColumnVector {
    var closeCount = 0

    override def getDataType: DataType = delegate.getDataType
    override def getSize: Int = delegate.getSize
    override def isNullAt(rowId: Int): Boolean = delegate.isNullAt(rowId)
    override def getBoolean(rowId: Int): Boolean = delegate.getBoolean(rowId)
    override def getInt(rowId: Int): Int = delegate.getInt(rowId)
    override def close(): Unit = closeCount += 1
  }
}
