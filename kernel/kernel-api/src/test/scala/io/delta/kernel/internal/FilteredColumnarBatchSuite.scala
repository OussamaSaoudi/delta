/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import java.lang.{Boolean => BooleanJ}
import java.util.Optional

import io.delta.kernel.TransactionSuite.columnarBatch
import io.delta.kernel.data.ColumnVector
import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types.{LongType, StructType}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FilteredColumnarBatchSuite extends AnyFunSuite with VectorTestUtils with Matchers {

  private val testSchema = new StructType().add("id", LongType.LONG)

  test("constructor should succeed when selectionVector is present and numSelectedRows is valid") {
    val data = columnarBatch(testSchema, Seq(longVector(Seq(0L, 1L, 2L, 3L, 4L))))
    val selectionVector = Optional.of(booleanVector(Seq(true, false, true, false, true)))
    val batch = new FilteredColumnarBatch(data, selectionVector, "/test/path", 3)

    assert(batch.getFilePath == Optional.of("/test/path"))
    assert(batch.getPreComputedNumSelectedRows == Optional.of(3))
    assert(batch.getData == data)
    assert(batch.getSelectionVector == selectionVector)
  }

  test(
    "constructor should succeed when selectionVector is empty and numSelectedRows " +
      "equals batch size") {
    val data = columnarBatch(testSchema, Seq(longVector(Seq(0L, 1L, 2L, 3L, 4L))))
    val selectionVector = Optional.empty[ColumnVector]()
    val batch = new FilteredColumnarBatch(data, selectionVector, "/test/path", 5)

    assert(batch.getFilePath == Optional.of("/test/path"))
    assert(batch.getPreComputedNumSelectedRows == Optional.of(5))
    assert(batch.getData == data)
    assert(batch.getSelectionVector == selectionVector)
  }

  test("constructor should throw IllegalArgumentException " +
    "when selectionVector is empty and numSelectedRows != batch size") {
    val data = columnarBatch(testSchema, Seq(longVector(Seq(0L, 1L, 2L, 3L, 4L))))
    val selectionVector = Optional.empty[ColumnVector]()
    val exMsg = intercept[IllegalArgumentException] {
      new FilteredColumnarBatch(data, selectionVector, "/test/path", 3)
    }.getMessage

    assert(exMsg.contains("Invalid precomputedNumSelectedRows"))
    assert(exMsg.contains("must be equal to batch size when selectionVector is empty"))
  }

  test("constructor should throw IllegalArgumentException " +
    "when selectionVector is present and numSelectedRows > batch size") {
    val data = columnarBatch(testSchema, Seq(longVector(Seq(0L, 1L, 2L, 3L))))
    val selectionVector = Optional.of(booleanVector(Seq(true, false, true, false)))

    val exMsg = intercept[IllegalArgumentException] {
      new FilteredColumnarBatch(data, selectionVector, "/test/path", 5)
    }.getMessage

    assert(exMsg.contains("Invalid precomputedNumSelectedRows"))
    assert(exMsg.contains("no larger than batch size"))
  }

  test("withSelectionVector intersects selections and treats null as unselected") {
    val data = columnarBatch(testSchema, Seq(longVector(Seq(0L, 1L, 2L, 3L))))
    val existing = booleanVector(Seq[BooleanJ](true, null, true, false))
    val additional = booleanVector(Seq[BooleanJ](true, true, false, true))
    val batch = new FilteredColumnarBatch(data, Optional.of(existing), "/test/path", 2)

    val selected = batch.withSelectionVector(additional)
    val selection = selected.getSelectionVector.get()

    assert(selected.getData eq data)
    assert(selected.getFilePath === Optional.of("/test/path"))
    assert(selected.getPreComputedNumSelectedRows === Optional.empty())
    assert((0 until 4).map(selection.getBoolean) === Seq(true, false, false, false))
    assert((0 until 4).forall(!selection.isNullAt(_)))
  }

  test("withSelectionVector returns a borrowed non-null view without an existing selection") {
    val data = columnarBatch(testSchema, Seq(longVector(Seq(0L, 1L))))
    val additional = booleanVector(Seq[BooleanJ](null, true))

    val selected = new FilteredColumnarBatch(data, Optional.empty()).withSelectionVector(additional)
    val selection = selected.getSelectionVector.get()

    assert(!(selection eq additional))
    assert((0 until 2).forall(!selection.isNullAt(_)))
    assert((0 until 2).map(selection.getBoolean) === Seq(false, true))
    selection.close()
    assert(additional.getBoolean(1))
  }

  test("withData preserves selection metadata for a same-size projection") {
    val data = columnarBatch(testSchema, Seq(longVector(Seq(0L, 1L, 2L))))
    val selection = booleanVector(Seq[BooleanJ](true, false, true))
    val batch = new FilteredColumnarBatch(data, Optional.of(selection), "/test/path", 2)
    val projectedSchema = new StructType().add("renamed", LongType.LONG)
    val projected = columnarBatch(projectedSchema, Seq(longVector(Seq(0L, 1L, 2L))))

    val result = batch.withData(projected)

    assert(result.getData eq projected)
    assert(result.getSelectionVector === Optional.of(selection))
    assert(result.getFilePath === Optional.of("/test/path"))
    assert(result.getPreComputedNumSelectedRows === Optional.of(2))
  }

  test("selection and replacement data must match the batch row count") {
    val data = columnarBatch(testSchema, Seq(longVector(Seq(0L, 1L))))

    intercept[IllegalArgumentException] {
      new FilteredColumnarBatch(data, Optional.of(booleanVector(Seq[BooleanJ](true))))
    }
    intercept[IllegalArgumentException] {
      new FilteredColumnarBatch(data, Optional.of(longVector(Seq(0L, 1L))))
    }
    intercept[IllegalArgumentException] {
      new FilteredColumnarBatch(data, Optional.empty()).withSelectionVector(
        booleanVector(Seq[BooleanJ](true)))
    }
    intercept[IllegalArgumentException] {
      new FilteredColumnarBatch(data, Optional.empty()).withData(
        columnarBatch(testSchema, Seq(longVector(Seq(0L)))))
    }
  }
}
