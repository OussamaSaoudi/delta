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

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ColumnarBatch, ColumnVector}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector
import io.delta.kernel.expressions.{Column, Expression, In, Literal, Predicate, ScalarExpression}
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class IntervalExpressionEvaluatorSuite extends AnyFunSuite with ExpressionSuiteBase {
  private val yearMonth = IntervalYearMonthType.INTERVAL_YEAR_MONTH
  private val dayTime = IntervalDayTimeType.INTERVAL_DAY_TIME

  private def scalar(name: String, children: Expression*): ScalarExpression =
    new ScalarExpression(name, children.asJava)

  private def vector(dataType: DataType, values: AnyRef*): ColumnVector =
    DefaultGenericVector.fromArray(dataType, values.toArray)

  private def batch(schema: StructType, columns: ColumnVector*): ColumnarBatch =
    new DefaultColumnarBatch(columns.head.getSize, schema, columns.toArray)

  private def evaluate(
      input: ColumnarBatch,
      expression: Expression,
      outputType: DataType): ColumnVector =
    new DefaultExpressionEvaluator(input.getSchema, expression, outputType).eval(input)

  test("interval literals, columns, COALESCE, and ARRAY preserve their logical types") {
    val empty = zeroColumnBatch(2)
    val ymLiteral = evaluate(empty, Literal.ofIntervalYearMonth(-13), yearMonth)
    val dtLiteral = evaluate(empty, Literal.ofIntervalDayTime(-86400000000L), dayTime)
    assert(ymLiteral.getIntervalYearMonth(1) === -13)
    assert(dtLiteral.getIntervalDayTime(1) === -86400000000L)

    val schema = new StructType()
      .add("ym", yearMonth)
      .add("dt", dayTime)
      .add("ymFallback", yearMonth)
      .add("dtFallback", dayTime)
    val input = batch(
      schema,
      vector(yearMonth, null, Int.box(30)),
      vector(dayTime, Long.box(-5L), null),
      vector(yearMonth, Int.box(-13), Int.box(99)),
      vector(dayTime, Long.box(7L), Long.box(11L)))

    val ym = evaluate(
      input,
      scalar("COALESCE", new Column("ym"), new Column("ymFallback")),
      yearMonth)
    val dt = evaluate(
      input,
      scalar("COALESCE", new Column("dt"), new Column("dtFallback")),
      dayTime)
    assert((0 until 2).map(ym.getIntervalYearMonth) === Seq(-13, 30))
    assert((0 until 2).map(dt.getIntervalDayTime) === Seq(-5L, 11L))

    val arrays = evaluate(
      input,
      scalar("ARRAY", new Column("ymFallback"), Literal.ofIntervalYearMonth(Int.MinValue)),
      new ArrayType(yearMonth, false))
    val elements = arrays.getArray(1).getElements
    assert(elements.getIntervalYearMonth(0) === 99)
    assert(elements.getIntervalYearMonth(1) === Int.MinValue)
  }

  test("interval comparisons and IN use signed logical ordering and SQL null semantics") {
    val schema = new StructType()
      .add("ym", yearMonth)
      .add("otherYm", yearMonth)
      .add("dt", dayTime)
      .add("otherDt", dayTime)
    val input = batch(
      schema,
      vector(yearMonth, Int.box(Int.MinValue), Int.box(-13), null),
      vector(yearMonth, Int.box(Int.MaxValue), Int.box(-13), null),
      vector(dayTime, Long.box(Long.MaxValue), Long.box(-5L), null),
      vector(dayTime, Long.box(Long.MinValue), Long.box(-5L), null))

    val ymLess = evaluate(
      input,
      new Predicate("<", new Column("ym"), new Column("otherYm")),
      BooleanType.BOOLEAN)
    val dtGreater = evaluate(
      input,
      new Predicate(">", new Column("dt"), new Column("otherDt")),
      BooleanType.BOOLEAN)
    assert(ymLess.getBoolean(0) && !ymLess.getBoolean(1) && ymLess.isNullAt(2))
    assert(dtGreater.getBoolean(0) && !dtGreater.getBoolean(1) && dtGreater.isNullAt(2))

    val in = new In(
      new Column("ym"),
      Seq[Expression](
        Literal.ofIntervalYearMonth(-13),
        Literal.ofNull(yearMonth),
        Literal.ofIntervalYearMonth(7)).asJava)
    val inResult = evaluate(input, in, BooleanType.BOOLEAN)
    assert(inResult.isNullAt(0))
    assert(inResult.getBoolean(1))
    assert(inResult.isNullAt(2))
  }

  test("all arithmetic operations retain interval types and null propagation") {
    val schema = new StructType()
      .add("leftYm", yearMonth)
      .add("rightYm", yearMonth)
      .add("leftDt", dayTime)
      .add("rightDt", dayTime)
    val input = batch(
      schema,
      vector(yearMonth, Int.box(30), null),
      vector(yearMonth, Int.box(-13), Int.box(2)),
      vector(dayTime, Long.box(86400000000L), null),
      vector(dayTime, Long.box(-5L), Long.box(2L)))

    val ymExpected = Seq("ADD" -> 17, "SUBTRACT" -> 43, "MULTIPLY" -> -390, "DIVIDE" -> -2)
    ymExpected.foreach { case (operation, expected) =>
      val result = evaluate(
        input,
        scalar(operation, new Column("leftYm"), new Column("rightYm")),
        yearMonth)
      assert(result.getDataType === yearMonth)
      assert(result.getIntervalYearMonth(0) === expected)
      assert(result.isNullAt(1))
    }

    val dtExpected = Seq(
      "ADD" -> 86399999995L,
      "SUBTRACT" -> 86400000005L,
      "MULTIPLY" -> -432000000000L,
      "DIVIDE" -> -17280000000L)
    dtExpected.foreach { case (operation, expected) =>
      val result = evaluate(
        input,
        scalar(operation, new Column("leftDt"), new Column("rightDt")),
        dayTime)
      assert(result.getDataType === dayTime)
      assert(result.getIntervalDayTime(0) === expected)
      assert(result.isNullAt(1))
    }
  }

  test("interval arithmetic reports overflow and invalid division lazily") {
    val cases = Seq(
      () =>
        evaluate(
          zeroColumnBatch(1),
          scalar(
            "ADD",
            Literal.ofIntervalYearMonth(Int.MaxValue),
            Literal.ofIntervalYearMonth(1)),
          yearMonth).getIntervalYearMonth(0),
      () =>
        evaluate(
          zeroColumnBatch(1),
          scalar(
            "DIVIDE",
            Literal.ofIntervalYearMonth(1),
            Literal.ofIntervalYearMonth(0)),
          yearMonth).getIntervalYearMonth(0),
      () =>
        evaluate(
          zeroColumnBatch(1),
          scalar(
            "MULTIPLY",
            Literal.ofIntervalDayTime(Long.MaxValue),
            Literal.ofIntervalDayTime(2)),
          dayTime).getIntervalDayTime(0),
      () =>
        evaluate(
          zeroColumnBatch(1),
          scalar(
            "DIVIDE",
            Literal.ofIntervalDayTime(Long.MinValue),
            Literal.ofIntervalDayTime(-1)),
          dayTime).getIntervalDayTime(0))

    cases.foreach(operation => assertThrows[ArithmeticException](operation()))
  }
}
