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
package io.delta.kernel.defaults.engine

import java.util.Optional

import io.delta.kernel.data.{ColumnVector, FilteredColumnarBatch}
import io.delta.kernel.data.FilteredColumnarBatch.Lifetime
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.ExpressionTestUtils
import io.delta.kernel.types.BooleanType.BOOLEAN
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.StructType

import org.scalatest.funsuite.AnyFunSuite

class DefaultExpressionHandlerSuite extends AnyFunSuite with ExpressionTestUtils {

  test("evaluate a struct expression as a batch") {
    val inputSchema = new StructType()
    val inputData = new DefaultColumnarBatch(2, inputSchema, Array.empty[ColumnVector])
    val selection = selectionVector(Array(true, false), 0, 2)
    val input = new FilteredColumnarBatch(inputData, Optional.of(selection), Lifetime.OWNED)
    val outputSchema = new StructType().add("value", INTEGER)

    val result = new DefaultExpressionHandler()
      .getEvaluator(inputSchema, struct(int(7)), outputSchema)
      .eval(input)

    assert(result.getData.getSchema === outputSchema)
    assert(result.getData.getSize === 2)
    assert(result.getData.getColumnVector(0).getInt(0) === 7)
    assert(result.getData.getColumnVector(0).getInt(1) === 7)
    assert(result.getSelectionVector === input.getSelectionVector)
    assert(result.isSelected(0))
    assert(!result.isSelected(1))
    assert(result.getLifetime === Lifetime.OWNED)
  }

  test("create selection vector: single value") {
    Seq(true, false).foreach { testValue =>
      val outputVector = selectionVector(Seq(testValue).toArray, 0, 1)
      assert(outputVector.getDataType === BOOLEAN)
      assert(outputVector.getSize == 1)
      assert(outputVector.isNullAt(0) == false)
      assert(outputVector.getBoolean(0) == testValue)
    }
  }

  test("create selection vector: multiple values array, partial array") {
    Seq(
      (0, testValues.length),
      (0, 3),
      (2, 2),
      (2, 4),
      (3, testValues.length),
      (testValues.length, testValues.length)).foreach { pair =>
      val (from, to) = (pair._1, pair._2)
      val outputVector = selectionVector(testValues, from, to)
      assert(outputVector.getDataType === BOOLEAN)
      assert(outputVector.getSize == (to - from))
      Seq.range(from, to).foreach { rowId =>
        assert(outputVector.isNullAt(rowId - from) == false)
        assert(outputVector.getBoolean(rowId - from) == testValues(rowId))
      }
    }
  }

  test("create selection vector: empty values") {
    assert(selectionVector(Array.empty[Boolean], 0, 0).getSize === 0)
  }

  test("create selection vector: update values array and expect no changes in output") {
    val outputVector = selectionVector(testValues, 0, testValues.length)
    // update the input values array and assert the value is not changed in the returned vector
    val oldValue = testValues(2)
    assert(oldValue == false)
    testValues(2) = true
    assert(outputVector.isNullAt(2) == false)
    assert(outputVector.getBoolean(2) == oldValue)
  }

  test("create selection vector: invalid to and/or from offset") {
    Seq((-1, 0), (3, 2), (2, testValues.length + 1), (testValues.length + 1, 100))
      .foreach { pair =>
        val (from, to) = (pair._1, pair._2)
        val ex = intercept[IllegalArgumentException] {
          selectionVector(testValues, from, to)
        }
        assert(ex.getMessage.contains(
          s"invalid range from=$from, to=$to, values length=${testValues.length}"))
      }
  }

  test("create selection vector: null values array") {
    val ex = intercept[NullPointerException] {
      selectionVector(null, 0, 25)
    }
    assert(ex.getMessage.contains("values is null"))
  }

  private def selectionVector(values: Array[Boolean], from: Int, to: Int) = {
    new DefaultExpressionHandler().createSelectionVector(values, from, to)
  }

  private val testValues = Seq(false, true, false, false, true, true).toArray
}
