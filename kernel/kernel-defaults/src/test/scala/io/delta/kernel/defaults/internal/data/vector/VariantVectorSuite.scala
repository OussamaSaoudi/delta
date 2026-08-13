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

import java.util

import io.delta.kernel.data.{ColumnVector, VariantValue}
import io.delta.kernel.internal.data.{GenericRow, StructRow}
import io.delta.kernel.types.{StructType, VariantType}

import org.scalatest.funsuite.AnyFunSuite

class VariantVectorSuite extends AnyFunSuite {

  test("DefaultVariantVector supports nulls, bounds, and Variant-only access") {
    val first = variant(1)
    val second = variant(2)
    val vector = new DefaultVariantVector(3, Array(first, null, second))

    assert(vector.getDataType == VariantType.VARIANT)
    assert(vector.getSize == 3)
    assert(vector.getVariant(0) == first)
    assert(vector.isNullAt(1))
    assert(vector.getVariant(1) == null)
    assert(vector.getVariant(2) == second)
    intercept[IllegalArgumentException](vector.getVariant(-1))
    intercept[IllegalArgumentException](vector.getVariant(3))
    intercept[UnsupportedOperationException](vector.getBinary(0))
  }

  test("Variant values propagate through generic, constant, view, and subfield vectors") {
    val first = variant(3)
    val second = variant(4)

    val generic = DefaultGenericVector.fromArray(
      VariantType.VARIANT,
      Array[Object](first, null, second))
    assert(generic.getVariant(0) == first)
    assert(generic.isNullAt(1))

    val constant = new DefaultConstantVector(VariantType.VARIANT, 2, second)
    assert(constant.getVariant(0) == second)
    assert(constant.getVariant(1) == second)

    val view = new DefaultViewVector(generic, 1, 3)
    assert(view.getSize == 2)
    assert(view.isNullAt(0))
    assert(view.getVariant(1) == second)
    intercept[IllegalArgumentException](view.getVariant(2))

    val structType = new StructType().add("payload", VariantType.VARIANT)
    val rowValues = new util.HashMap[Integer, Object]()
    rowValues.put(0, first)
    val row = new GenericRow(structType, rowValues)
    val subfield = new DefaultSubFieldVector(1, VariantType.VARIANT, 0, _ => row)
    assert(subfield.getVariant(0) == first)
  }

  test("ChildVectorBasedRow exposes Variant children") {
    val first = variant(5)
    val second = variant(6)
    val child: ColumnVector = new DefaultVariantVector(2, Array(first, second))
    val structType = new StructType().add("payload", VariantType.VARIANT)
    val structVector = new DefaultStructVector(
      2,
      structType,
      java.util.Optional.empty(),
      Array(child))

    assert(StructRow.fromStructVector(structVector, 0).getVariant(0) == first)
    assert(StructRow.fromStructVector(structVector, 1).getVariant(0) == second)
  }

  private def variant(seed: Byte): VariantValue =
    new VariantValue(Array(seed), Array((seed + 10).toByte))
}
