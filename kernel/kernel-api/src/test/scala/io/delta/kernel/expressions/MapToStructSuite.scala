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
package io.delta.kernel.expressions

import org.scalatest.funsuite.AnyFunSuite

class MapToStructSuite extends AnyFunSuite {
  test("exposes one immutable child") {
    val child = new Column("partitionValues")
    val expression = new MapToStruct(child)

    assert(expression.getMapExpression eq child)
    assert(expression.getChildren.size() == 1)
    assert(expression.getChildren.get(0) eq child)
    assert(expression.toString == "MAP_TO_STRUCT(column(`partitionValues`))")
    intercept[UnsupportedOperationException] {
      expression.getChildren.clear()
    }
  }

  test("rejects a null map expression") {
    intercept[NullPointerException] {
      new MapToStruct(null)
    }
  }
}
