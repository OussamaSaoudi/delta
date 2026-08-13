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

import java.lang.{Byte => ByteJ}
import java.lang.{Double => DoubleJ}
import java.lang.{Float => FloatJ}
import java.lang.{Integer => IntegerJ}
import java.lang.{Long => LongJ}
import java.lang.{Short => ShortJ}
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
    case _: ByteType => Literal.ofByte(value.asInstanceOf[ByteJ])
    case _: ShortType => Literal.ofShort(value.asInstanceOf[ShortJ])
    case _: IntegerType => Literal.ofInt(value.asInstanceOf[IntegerJ])
    case _: LongType => Literal.ofLong(value.asInstanceOf[LongJ])
    case _: FloatType => Literal.ofFloat(value.asInstanceOf[FloatJ])
    case _: DoubleType => Literal.ofDouble(value.asInstanceOf[DoubleJ])
    case _ => throw new IllegalArgumentException(s"Unsupported literal type: $dataType")
  }

  private def evaluateLiterals(
      operation: String,
      dataType: DataType,
      left: AnyRef,
      right: AnyRef): ColumnVector = {
    val expression = arithmetic(operation, literal(dataType, left), literal(dataType, right))
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
      PrimitiveCase(
        ByteType.BYTE,
        ByteJ.valueOf(12.toByte),
        ByteJ.valueOf(3.toByte),
        Seq(15.toByte, 9.toByte, 36.toByte, 4.toByte).map(ByteJ.valueOf)),
      PrimitiveCase(
        ShortType.SHORT,
        ShortJ.valueOf(120.toShort),
        ShortJ.valueOf(3.toShort),
        Seq(123.toShort, 117.toShort, 360.toShort, 40.toShort).map(ShortJ.valueOf)),
      PrimitiveCase(
        IntegerType.INTEGER,
        IntegerJ.valueOf(7),
        IntegerJ.valueOf(2),
        Seq(9, 5, 14, 3).map(IntegerJ.valueOf)),
      PrimitiveCase(
        LongType.LONG,
        LongJ.valueOf(21L),
        LongJ.valueOf(4L),
        Seq(25L, 17L, 84L, 5L).map(LongJ.valueOf)),
      PrimitiveCase(
        FloatType.FLOAT,
        FloatJ.valueOf(7.5f),
        FloatJ.valueOf(2.0f),
        Seq(9.5f, 5.5f, 15.0f, 3.75f).map(FloatJ.valueOf)),
      PrimitiveCase(
        DoubleType.DOUBLE,
        DoubleJ.valueOf(8.25d),
        DoubleJ.valueOf(2.5d),
        Seq(10.75d, 5.75d, 20.625d, 3.3d).map(DoubleJ.valueOf)))

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
          Array[AnyRef](IntegerJ.valueOf(2), IntegerJ.valueOf(4), null)),
        DefaultGenericVector.fromArray(
          IntegerType.INTEGER,
          Array[AnyRef](IntegerJ.valueOf(1), null, IntegerJ.valueOf(3)))))
    val expression = arithmetic(
      "MULTIPLY",
      arithmetic("ADD", new Column("left"), Literal.ofInt(3)),
      arithmetic("SUBTRACT", Literal.ofInt(10), new Column("right")))
    val result =
      new DefaultExpressionEvaluator(
        input.getSchema,
        expression,
        IntegerType.INTEGER).eval(input)

    checkValues(result, IntegerType.INTEGER, Seq(IntegerJ.valueOf(45), null, null))
  }

  test("integral arithmetic detects overflow") {
    val cases = Seq(
      (
        ByteType.BYTE,
        ByteJ.valueOf(Byte.MaxValue),
        ByteJ.valueOf(Byte.MinValue),
        ByteJ.valueOf(2.toByte),
        ByteJ.valueOf((-1).toByte)),
      (
        ShortType.SHORT,
        ShortJ.valueOf(Short.MaxValue),
        ShortJ.valueOf(Short.MinValue),
        ShortJ.valueOf(2.toShort),
        ShortJ.valueOf((-1).toShort)),
      (
        IntegerType.INTEGER,
        IntegerJ.valueOf(Int.MaxValue),
        IntegerJ.valueOf(Int.MinValue),
        IntegerJ.valueOf(2),
        IntegerJ.valueOf(-1)),
      (
        LongType.LONG,
        LongJ.valueOf(Long.MaxValue),
        LongJ.valueOf(Long.MinValue),
        LongJ.valueOf(2L),
        LongJ.valueOf(-1L)))

    cases.foreach { case (dataType, max, min, two, negativeOne) =>
      Seq(
        ("ADD", max, one(dataType)),
        ("SUBTRACT", min, one(dataType)),
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
      (ByteType.BYTE, ByteJ.valueOf(7.toByte), ByteJ.valueOf(0.toByte)),
      (ShortType.SHORT, ShortJ.valueOf(7.toShort), ShortJ.valueOf(0.toShort)),
      (IntegerType.INTEGER, IntegerJ.valueOf(7), IntegerJ.valueOf(0)),
      (LongType.LONG, LongJ.valueOf(7L), LongJ.valueOf(0L))).foreach {
      case (dataType, numerator, zero) =>
        intercept[ArithmeticException] {
          getValueAsObject(evaluateLiterals("DIVIDE", dataType, numerator, zero), 0)
        }
    }

    val floatInfinity = evaluateLiterals(
      "DIVIDE",
      FloatType.FLOAT,
      FloatJ.valueOf(7.0f),
      FloatJ.valueOf(0.0f))
    assert(floatInfinity.getFloat(0) === Float.PositiveInfinity)
    val doubleNaN = evaluateLiterals(
      "DIVIDE",
      DoubleType.DOUBLE,
      DoubleJ.valueOf(0.0d),
      DoubleJ.valueOf(0.0d))
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

  private def one(dataType: DataType): AnyRef = dataType match {
    case _: ByteType => ByteJ.valueOf(1.toByte)
    case _: ShortType => ShortJ.valueOf(1.toShort)
    case _: IntegerType => IntegerJ.valueOf(1)
    case _: LongType => LongJ.valueOf(1L)
    case _ => throw new IllegalArgumentException(s"Unsupported integral type: $dataType")
  }

  private def trackingIntVector(closeCount: AtomicInteger): ColumnVector = new ColumnVector {
    override def getDataType: DataType = IntegerType.INTEGER
    override def getSize: Int = 1
    override def close(): Unit = closeCount.incrementAndGet()
    override def isNullAt(rowId: Int): Boolean = false
    override def getInt(rowId: Int): Int = 1
  }
}
