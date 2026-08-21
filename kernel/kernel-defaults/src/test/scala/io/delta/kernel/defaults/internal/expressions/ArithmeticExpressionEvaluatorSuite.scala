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
import java.util.concurrent.atomic.AtomicInteger

import io.delta.kernel.data.ColumnVector
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector
import io.delta.kernel.defaults.utils.DefaultKernelTestUtils.getValueAsObject
import io.delta.kernel.expressions.{Column, Expression, Literal, ScalarExpression}
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class ArithmeticExpressionEvaluatorSuite extends AnyFunSuite with ExpressionSuiteBase {
  private val operations = Seq("ADD", "SUBTRACT", "MULTIPLY", "DIVIDE")

  private case class PrimitiveCase(
      dataType: DataType,
      left: AnyRef,
      right: AnyRef,
      expected: Seq[AnyRef])

  private def primitiveCase[T <: AnyVal](
      dataType: DataType,
      left: T,
      right: T,
      expected: T*): PrimitiveCase =
    PrimitiveCase(
      dataType,
      left.asInstanceOf[AnyRef],
      right.asInstanceOf[AnyRef],
      expected.map(_.asInstanceOf[AnyRef]))

  private def primitiveValues[T <: AnyVal](values: T*): Seq[AnyRef] =
    values.map(_.asInstanceOf[AnyRef])

  private def arithmetic(
      operation: String,
      left: Expression,
      right: Expression): ScalarExpression = {
    new ScalarExpression(operation, util.Arrays.asList(left, right))
  }

  private def checkValues(
      actual: ColumnVector,
      expectedType: DataType,
      expected: Seq[AnyRef]): Unit = {
    assert(actual.getDataType === expectedType)
    assert(actual.getSize === expected.size)
    expected.indices.foreach { rowId =>
      assert(actual.isNullAt(rowId) === (expected(rowId) == null))
      if (expected(rowId) != null) {
        assert(getValueAsObject(actual, rowId) === expected(rowId))
      }
    }
  }

  private def literal(dataType: DataType, value: AnyRef): Literal = dataType match {
    case _: ByteType => Literal.ofByte(value.asInstanceOf[Byte])
    case _: ShortType => Literal.ofShort(value.asInstanceOf[Short])
    case _: IntegerType => Literal.ofInt(value.asInstanceOf[Int])
    case _: LongType => Literal.ofLong(value.asInstanceOf[Long])
    case _: FloatType => Literal.ofFloat(value.asInstanceOf[Float])
    case _: DoubleType => Literal.ofDouble(value.asInstanceOf[Double])
    case _ => throw new IllegalArgumentException(s"Unsupported literal type: $dataType")
  }

  private def evaluateLiterals[T](
      operation: String,
      dataType: DataType,
      left: T,
      right: T): ColumnVector = {
    val expression = arithmetic(
      operation,
      literal(dataType, left.asInstanceOf[AnyRef]),
      literal(dataType, right.asInstanceOf[AnyRef]))
    new DefaultExpressionEvaluator(new StructType(), expression, dataType).eval(zeroColumnBatch(1))
  }

  private def assertUnsupported(
      expression: ScalarExpression,
      outputType: DataType,
      message: String): Unit = {
    val error = intercept[UnsupportedOperationException] {
      new DefaultExpressionEvaluator(new StructType(), expression, outputType)
    }
    assert(error.getMessage.contains(message))
  }

  test("evaluate primitive arithmetic") {
    val cases = Seq(
      primitiveCase[Byte](ByteType.BYTE, 12, 3, 15, 9, 36, 4),
      primitiveCase[Short](ShortType.SHORT, 120, 3, 123, 117, 360, 40),
      primitiveCase[Int](IntegerType.INTEGER, 7, 2, 9, 5, 14, 3),
      primitiveCase[Long](LongType.LONG, 21, 4, 25, 17, 84, 5),
      primitiveCase[Float](FloatType.FLOAT, 7.5f, 2.0f, 9.5f, 5.5f, 15.0f, 3.75f),
      primitiveCase[Double](DoubleType.DOUBLE, 8.25, 2.5, 10.75, 5.75, 20.625, 3.3))

    cases.foreach { testCase =>
      operations.zip(testCase.expected).foreach { case (operation, expected) =>
        checkValues(
          evaluateLiterals(operation, testCase.dataType, testCase.left, testCase.right),
          testCase.dataType,
          Seq(expected))
      }
    }
  }

  test("evaluate nested and null-propagating arithmetic") {
    val schema = new StructType()
      .add("left", IntegerType.INTEGER)
      .add("right", IntegerType.INTEGER)
    val input = new DefaultColumnarBatch(
      3,
      schema,
      Array(
        DefaultGenericVector.fromArray(
          IntegerType.INTEGER,
          Array[AnyRef](Int.box(2), Int.box(4), null)),
        DefaultGenericVector.fromArray(
          IntegerType.INTEGER,
          Array[AnyRef](Int.box(1), null, Int.box(3)))))
    val expression = arithmetic(
      "MULTIPLY",
      arithmetic("ADD", new Column("left"), Literal.ofInt(3)),
      arithmetic("SUBTRACT", Literal.ofInt(10), new Column("right")))
    val result =
      new DefaultExpressionEvaluator(
        input.getSchema,
        expression,
        IntegerType.INTEGER).eval(input)

    checkValues(result, IntegerType.INTEGER, Seq(Int.box(45), null, null))
  }

  test("integral arithmetic detects overflow") {
    val cases = Seq(
      (
        ByteType.BYTE,
        primitiveValues[Byte](Byte.MaxValue, Byte.MinValue, 2, -1, 1)),
      (
        ShortType.SHORT,
        primitiveValues[Short](Short.MaxValue, Short.MinValue, 2, -1, 1)),
      (
        IntegerType.INTEGER,
        primitiveValues[Int](Int.MaxValue, Int.MinValue, 2, -1, 1)),
      (
        LongType.LONG,
        primitiveValues[Long](Long.MaxValue, Long.MinValue, 2, -1, 1)))

    cases.foreach { case (dataType, Seq(max, min, two, negativeOne, one)) =>
      Seq(
        ("ADD", max, one),
        ("SUBTRACT", min, one),
        ("MULTIPLY", max, two),
        ("DIVIDE", min, negativeOne)).foreach { case (operation, left, right) =>
        intercept[ArithmeticException] {
          getValueAsObject(evaluateLiterals(operation, dataType, left, right), 0)
        }
      }
    }
  }

  test("division follows integral and floating-point semantics") {
    Seq(
      (ByteType.BYTE, primitiveValues[Byte](7, 0)),
      (ShortType.SHORT, primitiveValues[Short](7, 0)),
      (IntegerType.INTEGER, primitiveValues[Int](7, 0)),
      (LongType.LONG, primitiveValues[Long](7, 0))).foreach {
      case (dataType, Seq(numerator, zero)) =>
        intercept[ArithmeticException] {
          getValueAsObject(evaluateLiterals("DIVIDE", dataType, numerator, zero), 0)
        }
    }

    val floatInfinity = evaluateLiterals(
      "DIVIDE",
      FloatType.FLOAT,
      7.0f,
      0.0f)
    assert(floatInfinity.getFloat(0) === Float.PositiveInfinity)
    val doubleNaN = evaluateLiterals(
      "DIVIDE",
      DoubleType.DOUBLE,
      0.0d,
      0.0d)
    assert(doubleNaN.getDouble(0).isNaN)
  }

  test("validate arithmetic shape and types") {
    operations.foreach { operation =>
      Seq(
        util.Collections.emptyList[Expression](),
        util.Arrays.asList[Expression](Literal.ofInt(1)),
        util.Arrays.asList[Expression](Literal.ofInt(1), Literal.ofInt(2), Literal.ofInt(3)))
        .foreach { children =>
          assertUnsupported(
            new ScalarExpression(operation, children),
            IntegerType.INTEGER,
            s"$operation requires exactly two arguments")
        }

      assertUnsupported(
        arithmetic(operation, Literal.ofInt(1), Literal.ofLong(2L)),
        IntegerType.INTEGER,
        "arguments of the same type")
      assertUnsupported(
        arithmetic(operation, Literal.ofBoolean(true), Literal.ofBoolean(false)),
        BooleanType.BOOLEAN,
        "only supported for numeric types")
    }

    assertUnsupported(
      arithmetic("ADD", Literal.ofInt(1), Literal.ofInt(2)),
      LongType.LONG,
      "does not match expected output type")
  }

  test("arithmetic results borrow input vectors") {
    val closeCount = new AtomicInteger()
    val inputVector = trackingIntVector(closeCount)
    val schema = new StructType().add("value", IntegerType.INTEGER)
    val input = new DefaultColumnarBatch(1, schema, Array(inputVector))
    val sharedColumn = new Column("value")
    val expression = arithmetic(
      "MULTIPLY",
      arithmetic("ADD", sharedColumn, sharedColumn),
      arithmetic("SUBTRACT", sharedColumn, Literal.ofInt(1)))
    val result =
      new DefaultExpressionEvaluator(schema, expression, IntegerType.INTEGER).eval(input)

    assert(result.getInt(0) === 0)
    result.close()
    assert(closeCount.get() === 0)
    assert(inputVector.getInt(0) === 1)
  }

  private def trackingIntVector(closeCount: AtomicInteger): ColumnVector = new ColumnVector {
    override def getDataType: DataType = IntegerType.INTEGER
    override def getSize: Int = 1
    override def close(): Unit = closeCount.incrementAndGet()
    override def isNullAt(rowId: Int): Boolean = false
    override def getInt(rowId: Int): Int = 1
  }
}
