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
package io.delta.kernel.internal.data

import io.delta.kernel.data.ColumnVector
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray
import io.delta.kernel.types.LongType

import org.scalatest.funsuite.AnyFunSuite

class SelectionColumnVectorSuite extends AnyFunSuite {
  test("owning and borrowing selections close row indices according to ownership") {
    var owningCloses = 0
    var borrowingCloses = 0
    val owningIndices = trackingVector(owningCloses += 1)
    val borrowedIndices = trackingVector(borrowingCloses += 1)

    new SelectionColumnVector(RoaringBitmapArray.create(), owningIndices).close()
    SelectionColumnVector.borrowing(RoaringBitmapArray.create(), borrowedIndices).close()

    assert(owningCloses === 1)
    assert(borrowingCloses === 0)
  }

  private def trackingVector(onClose: => Unit): ColumnVector = new ColumnVector {
    override def getDataType = LongType.LONG
    override def getSize = 1
    override def close(): Unit = onClose
    override def isNullAt(rowId: Int) = false
    override def getLong(rowId: Int) = 0L
  }
}
