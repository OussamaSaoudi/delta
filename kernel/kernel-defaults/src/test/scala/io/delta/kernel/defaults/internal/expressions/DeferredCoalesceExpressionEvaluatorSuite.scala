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

import java.lang.{Integer => IntegerJ, Long => LongJ}
import java.util.function.Function

import scala.collection.JavaConverters._

import io.delta.kernel.data.ColumnVector
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector
import io.delta.kernel.expressions.{Column, Expression, Literal, ScalarExpression}
import io.delta.kernel.types.{DataType, IntegerType, LongType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class DeferredCoalesceExpressionEvaluatorSuite extends AnyFunSuite {
  test("fully resolved first child skips an invalid second child") {
    val input = batch(("first", IntegerType.INTEGER, Seq(IntegerJ.valueOf(1), IntegerJ.valueOf(2))))
    val result = evaluate(
      input,
      coalesce(new Column("first"), new Column("missing")),
      IntegerType.INTEGER)

    assertInts(result, Seq(IntegerJ.valueOf(1), IntegerJ.valueOf(2)))
  }

  test("complementary children skip an invalid third child") {
    val input = batch(
      ("first", IntegerType.INTEGER, Seq(IntegerJ.valueOf(1), null)),
      ("second", IntegerType.INTEGER, Seq(null, IntegerJ.valueOf(2))))
    val result = evaluate(
      input,
      coalesce(new Column("first"), new Column("second"), new Column("missing")),
      IntegerType.INTEGER)

    assertInts(result, Seq(IntegerJ.valueOf(1), IntegerJ.valueOf(2)))
  }

  test("unresolved rows reach an invalid child") {
    val input = batch(("first", IntegerType.INTEGER, Seq(IntegerJ.valueOf(1), null)))
    val evaluator = new DefaultExpressionEvaluator(
      input.getSchema,
      coalesce(new Column("first"), new Column("missing")),
      IntegerType.INTEGER)

    val error = intercept[IllegalArgumentException] {
      evaluator.eval(input)
    }
    assert(error.getMessage.contains("column(`missing`) doesn't exist"))
  }

  test("reached children must have the coalesce output type") {
    val input = batch(
      ("first", IntegerType.INTEGER, Seq(null)),
      ("second", LongType.LONG, Seq(LongJ.valueOf(2L))))
    val evaluator = new DefaultExpressionEvaluator(
      input.getSchema,
      coalesce(new Column("first"), new Column("second")),
      IntegerType.INTEGER)

    val error = intercept[UnsupportedOperationException] {
      evaluator.eval(input)
    }
    assert(error.getMessage.contains("Coalesce is only supported for arguments of the same type"))
  }

  test("coalesce nested in arithmetic remains lazy") {
    val input = batch(("first", IntegerType.INTEGER, Seq(IntegerJ.valueOf(1), IntegerJ.valueOf(2))))
    val expression = new ScalarExpression(
      "ADD",
      Seq[Expression](
        coalesce(new Column("first"), new Column("missing")),
        Literal.ofInt(10)).asJava)

    val result = evaluate(input, expression, IntegerType.INTEGER)
    assertInts(result, Seq(IntegerJ.valueOf(11), IntegerJ.valueOf(12)))
  }

  test("combination vector closes child vectors exactly once") {
    val first = new TrackingIntVector(Seq(IntegerJ.valueOf(1), null))
    val second = new TrackingIntVector(Seq(null, IntegerJ.valueOf(2)))
    val result = DefaultExpressionUtils.combinationVector(
      Seq[ColumnVector](first, second).asJava,
      new Function[IntegerJ, IntegerJ] {
        override def apply(rowId: IntegerJ): IntegerJ = rowId
      })

    assertInts(result, Seq(IntegerJ.valueOf(1), IntegerJ.valueOf(2)))
    result.close()
    result.close()

    assert(first.closeCount === 1)
    assert(second.closeCount === 1)
  }

  private def coalesce(children: Expression*): ScalarExpression = {
    new ScalarExpression("COALESCE", children.toList.asJava)
  }

  private def evaluate(
      input: DefaultColumnarBatch,
      expression: Expression,
      outputType: DataType): ColumnVector = {
    new DefaultExpressionEvaluator(input.getSchema, expression, outputType).eval(input)
  }

  private def batch(columns: (String, DataType, Seq[AnyRef])*): DefaultColumnarBatch = {
    val size = columns.head._3.size
    val schema = columns.foldLeft(new StructType()) {
      case (result, (name, dataType, _)) => result.add(name, dataType)
    }
    val vectors = columns.map { case (_, dataType, values) =>
      DefaultGenericVector.fromArray(dataType, values.toArray)
    }
    new DefaultColumnarBatch(size, schema, vectors.toArray)
  }

  private def assertInts(actual: ColumnVector, expected: Seq[IntegerJ]): Unit = {
    assert(actual.getDataType === IntegerType.INTEGER)
    assert(actual.getSize === expected.size)
    expected.indices.foreach { rowId =>
      assert(actual.isNullAt(rowId) === (expected(rowId) == null))
      if (expected(rowId) != null) {
        assert(actual.getInt(rowId) === expected(rowId).intValue())
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
