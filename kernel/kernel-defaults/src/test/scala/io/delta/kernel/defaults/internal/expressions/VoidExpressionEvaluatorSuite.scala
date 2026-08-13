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

import scala.collection.JavaConverters._

import io.delta.kernel.data.{ColumnarBatch, ColumnVector}
import io.delta.kernel.defaults.internal.data.{DefaultColumnarBatch, DefaultJsonRow}
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector
import io.delta.kernel.expressions.{Column, Expression, Literal, Predicate, ScalarExpression}
import io.delta.kernel.types.{BooleanType, DataType, StructType, VoidType}

import org.scalatest.funsuite.AnyFunSuite

class VoidExpressionEvaluatorSuite extends AnyFunSuite with ExpressionSuiteBase {
  private def evaluate(
      input: ColumnarBatch,
      expression: Expression,
      outputType: DataType): ColumnVector = {
    new DefaultExpressionEvaluator(input.getSchema, expression, outputType).eval(input)
  }

  test("void literals produce correctly sized all-null vectors") {
    Seq(0, 3).foreach { size =>
      val result = evaluate(zeroColumnBatch(size), Literal.ofVoid(), VoidType.VOID)
      assert(result.getDataType eq VoidType.VOID)
      assert(result.getSize === size)
      assert((0 until size).forall(result.isNullAt))
    }
  }

  test("void columns support IS_NULL and lazy all-void COALESCE") {
    var inputCloseCount = 0
    val inputVector = new ColumnVector {
      override def getDataType: DataType = VoidType.VOID
      override def getSize: Int = 3
      override def close(): Unit = inputCloseCount += 1
      override def isNullAt(rowId: Int): Boolean = true
    }
    val schema = new StructType().add("v", VoidType.VOID, false)
    val input = new DefaultColumnarBatch(3, schema, Array(inputVector))

    val column = evaluate(input, new Column("v"), VoidType.VOID)
    assert(column.getDataType eq VoidType.VOID)
    assert((0 until column.getSize).forall(column.isNullAt))

    val isNull = evaluate(
      input,
      new Predicate("IS_NULL", new Column("v")),
      BooleanType.BOOLEAN)
    assert((0 until isNull.getSize).forall(isNull.getBoolean))

    val coalesce = new ScalarExpression(
      "COALESCE",
      Seq[Expression](new Column("v"), Literal.ofVoid()).asJava)
    val coalesced = evaluate(input, coalesce, VoidType.VOID)
    assert(coalesced.getDataType eq VoidType.VOID)
    assert((0 until coalesced.getSize).forall(coalesced.isNullAt))

    coalesced.close()
    coalesced.close()
    assert(inputCloseCount === 0)
  }

  test("default vectors reject non-null void payloads") {
    val error = intercept[IllegalArgumentException] {
      DefaultGenericVector.fromArray(VoidType.VOID, Array[AnyRef](null, "payload"))
    }
    assert(error.getMessage.contains("rowId 1 must be null"))
  }

  test("JSON rows accept null or missing void fields and reject payloads") {
    val schema = new StructType()
      .add("nullValue", VoidType.VOID, false)
      .add("missingValue", VoidType.VOID, false)
    val row = DefaultJsonRow.fromJson("""{"nullValue":null}""", schema)
    assert(row.isNullAt(0))
    assert(row.isNullAt(1))

    val error = intercept[RuntimeException] {
      DefaultJsonRow.fromJson("""{"nullValue":1}""", schema)
    }
    assert(error.getMessage.contains("expected null"))
  }
}
