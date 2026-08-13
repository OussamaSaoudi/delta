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

class ColumnSuite extends AnyFunSuite {
  test("Column defensively copies its path") {
    val source = Array("file", "path")
    val column = new Column(source)

    source(1) = "changed"
    assert(column.getNames.toSeq === Seq("file", "path"))

    val returned = column.getNames
    returned(0) = "changed"
    assert(column.getNames.toSeq === Seq("file", "path"))
  }

  test("Column equality and nested append are unaffected by defensive copies") {
    val column = new Column(Array("file", "path"))

    assert(column === new Column(Array("file", "path")))
    assert(column.hashCode === new Column(Array("file", "path")).hashCode)
    assert(column.appendNestedField("leaf").getNames.toSeq === Seq("file", "path", "leaf"))
  }
}
