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
package io.delta.kernel.internal.util

import io.delta.kernel.data.{ColumnarBatch, ColumnVector}
import io.delta.kernel.expressions.Column
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types.{DataType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class ColumnBindingSuite extends AnyFunSuite with VectorTestUtils {
  private val nestedType = new StructType().add("value", StringType.STRING)
  private val schema = new StructType().add("nested", nestedType)
  private val values = stringVector(Seq("a", "b"))
  private val nested = new ColumnVector {
    override def getDataType: DataType = nestedType
    override def getSize: Int = values.getSize
    override def close(): Unit = {}
    override def isNullAt(rowId: Int): Boolean = false
    override def getChild(ordinal: Int): ColumnVector = values
  }
  private val batch = new ColumnarBatch {
    override def getSchema: StructType = schema
    override def getSize: Int = values.getSize
    override def getColumnVector(ordinal: Int): ColumnVector = nested
  }

  test("resolve nested columns once and retrieve their borrowed vectors") {
    val binding = ColumnBinding.resolve(schema, new Column(Array("nested", "value")))

    assert(binding.getDataType === StringType.STRING)
    assert(binding.getOrdinals.sameElements(Array(0, 0)))
    assert(binding.getVector(batch) eq values)
    assert(binding.getVector(batch).getString(1) === "b")
  }

  test("reject unresolved and empty paths") {
    intercept[IllegalArgumentException] {
      ColumnBinding.resolve(schema, new Column(Array("nested", "missing")))
    }
    intercept[IllegalArgumentException] {
      ColumnBinding.resolve(schema, new Column(Array.empty[String]))
    }
  }

  test("Column paths are immutable after binding") {
    val names = Array("nested", "value")
    val column = new Column(names)
    val binding = ColumnBinding.resolve(schema, column)

    names(0) = "changed"
    column.getNames()(0) = "changed"
    assert(binding.getVector(batch) eq values)
    assert(column === new Column(Array("nested", "value")))
  }
}
