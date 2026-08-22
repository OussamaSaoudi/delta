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
package io.delta.kernel.defaults.internal.data.vector

import java.util.Optional

import io.delta.kernel.data.ColumnVector
import io.delta.kernel.types.{IntegerType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class DefaultViewVectorSuite extends AnyFunSuite {

  test("getChild reuses views and preserves nullable ancestors") {
    val leaf = new DefaultIntVector(
      IntegerType.INTEGER,
      3,
      Optional.empty(),
      Array(10, 20, 30))
    val nestedType = new StructType().add("value", IntegerType.INTEGER)
    val nested = new DefaultStructVector(
      3,
      nestedType,
      Optional.of(Array(false, true, false)),
      Array[ColumnVector](leaf))
    val rootType = new StructType().add("nested", nestedType)
    val root = new DefaultStructVector(
      3,
      rootType,
      Optional.of(Array(true, false, false)),
      Array[ColumnVector](nested))
    val view = new DefaultViewVector(root, 0, 3)

    val firstNested = view.getChild(0)
    assert(firstNested eq view.getChild(0))
    val firstLeaf = firstNested.getChild(0)
    assert(firstLeaf eq firstNested.getChild(0))
    assert(firstLeaf.isNullAt(0))
    assert(firstLeaf.isNullAt(1))
    assert(firstLeaf.getInt(2) === 30)
  }

  test("cached child views stay non-owning and preserve invalid access") {
    var rootClosed = false
    var childClosed = false
    val child = new DefaultIntVector(
      IntegerType.INTEGER,
      1,
      Optional.empty(),
      Array(1)) {
      override def close(): Unit = childClosed = true
    }
    val schema = new StructType().add("value", IntegerType.INTEGER)
    val root = new DefaultStructVector(
      1,
      schema,
      Optional.empty(),
      Array[ColumnVector](child)) {
      override def close(): Unit = rootClosed = true
    }
    val view = new DefaultViewVector(root, 0, 1)
    val childView = view.getChild(0)

    view.close()
    childView.close()
    assert(!rootClosed && !childClosed)
    intercept[IllegalArgumentException](view.getChild(-1))
    intercept[IllegalArgumentException](view.getChild(1))
    intercept[UnsupportedOperationException](childView.getChild(0))
  }
}
