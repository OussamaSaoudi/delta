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
package io.delta.kernel.data

import java.util

import scala.collection.JavaConverters._

import io.delta.kernel.internal.data.{DelegateRow, GenericColumnVector, GenericRow}
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types.{IntegerType, StructType, VariantType}

import org.scalatest.funsuite.AnyFunSuite

class VariantValueSuite extends AnyFunSuite {

  test("VariantValue owns its buffers and compares their contents") {
    val valueBytes = Array[Byte](1, 2, 3)
    val metadataBytes = Array[Byte](4, 5)
    val variant = new VariantValue(valueBytes, metadataBytes)

    valueBytes(0) = 99
    metadataBytes(0) = 98
    assert(variant.getValue.sameElements(Array[Byte](1, 2, 3)))
    assert(variant.getMetadata.sameElements(Array[Byte](4, 5)))

    val returnedValue = variant.getValue
    val returnedMetadata = variant.getMetadata
    returnedValue(0) = 97
    returnedMetadata(0) = 96
    assert(variant.getValue.sameElements(Array[Byte](1, 2, 3)))
    assert(variant.getMetadata.sameElements(Array[Byte](4, 5)))

    val equal = new VariantValue(Array[Byte](1, 2, 3), Array[Byte](4, 5))
    assert(variant == equal)
    assert(variant.hashCode == equal.hashCode)
    assert(variant != new VariantValue(Array[Byte](1), Array[Byte](4, 5)))
  }

  test("generic rows and vectors expose Variant values") {
    val first = new VariantValue(Array[Byte](1), Array[Byte](10))
    val second = new VariantValue(Array[Byte](2), Array[Byte](20))
    val vector = new GenericColumnVector(List(first, null, second).asJava, VariantType.VARIANT)

    assert(vector.getVariant(0) == first)
    assert(vector.isNullAt(1))
    assert(vector.getVariant(2) == second)
    assert(VectorUtils.getValueAsObject(vector, VariantType.VARIANT, 0) == first)
    assert(VectorUtils.getValueAsObject(vector, VariantType.VARIANT, 1) == null)

    val schema = new StructType()
      .add("id", IntegerType.INTEGER)
      .add("payload", VariantType.VARIANT)
    val values = new util.HashMap[Integer, Object]()
    values.put(0, Integer.valueOf(7))
    values.put(1, first)
    val row = new GenericRow(schema, values)
    assert(row.getVariant(1) == first)

    val delegated = new DelegateRow(
      row,
      Map[Integer, Object](Integer.valueOf(1) -> second.asInstanceOf[Object]).asJava)
    assert(delegated.getVariant(1) == second)
  }

  test("Variant values propagate through nested generic vectors") {
    val variant = new VariantValue(Array[Byte](8), Array[Byte](9))
    val nestedType = new StructType().add("payload", VariantType.VARIANT)
    val rowValues = new util.HashMap[Integer, Object]()
    rowValues.put(0, variant)
    val row = new GenericRow(nestedType, rowValues)
    val structVector = new GenericColumnVector(List[Row](row).asJava, nestedType)

    val child = structVector.getChild(0)
    assert(child.getDataType == VariantType.VARIANT)
    assert(child.getVariant(0) == variant)
  }
}
