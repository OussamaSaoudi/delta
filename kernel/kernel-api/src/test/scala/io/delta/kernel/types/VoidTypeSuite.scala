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
package io.delta.kernel.types

import java.lang.{Integer => IntegerJ}
import java.util

import scala.collection.JavaConverters._

import io.delta.kernel.data.ColumnVector
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.data.{GenericColumnVector, GenericRow}
import io.delta.kernel.internal.types.DataTypeJsonSerDe
import io.delta.kernel.internal.util.{SchemaUtils, VectorUtils}

import org.scalatest.funsuite.AnyFunSuite

class VoidTypeSuite extends AnyFunSuite {
  test("void is a singleton primitive type with an exact literal factory") {
    assert(VoidType.VOID.toString === "void")
    assert(!VoidType.VOID.isNested)
    assert(BasePrimitiveType.createPrimitive("void") eq VoidType.VOID)
    assert(BasePrimitiveType.getAllPrimitiveTypes.contains(VoidType.VOID))

    val literal = Literal.ofVoid()
    assert(literal.getDataType eq VoidType.VOID)
    assert(literal.getValue === null)
    assert(literal === Literal.ofNull(VoidType.VOID))
  }

  test("void vectors only contain nulls") {
    val vector = new GenericColumnVector(Seq[AnyRef](null, null).asJava, VoidType.VOID)
    assert(vector.getDataType eq VoidType.VOID)
    assert((0 until vector.getSize).forall(vector.isNullAt))
    assert((0 until vector.getSize).forall { rowId =>
      VectorUtils.getValueAsObject(vector, VoidType.VOID, rowId) == null
    })

    val error = intercept[IllegalArgumentException] {
      new GenericColumnVector(Seq[AnyRef](null, "payload").asJava, VoidType.VOID)
    }
    assert(error.getMessage.contains("rowId 1 must be null"))

    val invalidVector = new ColumnVector {
      override def getDataType: DataType = VoidType.VOID
      override def getSize: Int = 1
      override def close(): Unit = {}
      override def isNullAt(rowId: Int): Boolean = false
    }
    assert(intercept[IllegalArgumentException] {
      VectorUtils.getValueAsObject(invalidVector, VoidType.VOID, 0)
    }.getMessage.contains("must be null"))
  }

  test("sparse and dense rows only contain null void values") {
    val schema = new StructType().add("v", VoidType.VOID, false)
    assert(new GenericRow(schema, new util.HashMap[IntegerJ, Object]()).isNullAt(0))
    assert(GenericRow.fromValues(schema, Seq[AnyRef](null).asJava).isNullAt(0))

    val sparseValues = new util.HashMap[IntegerJ, Object]()
    sparseValues.put(IntegerJ.valueOf(0), "payload")
    assert(intercept[IllegalArgumentException] {
      new GenericRow(schema, sparseValues)
    }.getMessage.contains("ordinal 0 must be null"))
    assert(intercept[IllegalArgumentException] {
      GenericRow.fromValues(schema, Seq[AnyRef]("payload").asJava)
    }.getMessage.contains("ordinal 0 must be null"))
  }

  test("persisted Delta schemas reject void recursively") {
    val cases = Seq[DataType](
      VoidType.VOID,
      new StructType().add("nested", VoidType.VOID),
      new ArrayType(VoidType.VOID, true),
      new MapType(StringType.STRING, VoidType.VOID, true))

    cases.foreach { dataType =>
      val error = intercept[KernelException] {
        SchemaUtils.validateSchema(
          new StructType().add("value", dataType),
          false,
          false,
          false)
      }
      assert(error.getMessage.contains("void"))
    }

    val error = intercept[KernelException] {
      DataTypeJsonSerDe.deserializeStructType(
        """{"type":"struct","fields":[{"name":"v","type":"void","nullable":true,
          |"metadata":{}}]}""".stripMargin)
    }
    assert(error.getMessage.toLowerCase(java.util.Locale.ROOT).contains("void"))
  }
}
