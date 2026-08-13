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

import java.util

import scala.collection.JavaConverters._

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.types.DataTypeJsonSerDe
import io.delta.kernel.internal.util.{SchemaUtils, VectorUtils}
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class IntervalDataSuite extends AnyFunSuite {
  private val yearMonth = IntervalYearMonthType.INTERVAL_YEAR_MONTH
  private val dayTime = IntervalDayTimeType.INTERVAL_DAY_TIME

  test("interval primitive aliases deserialize and serialize canonically") {
    Seq("interval year", "interval month", "interval year to month").foreach { name =>
      assert(BasePrimitiveType.createPrimitive(name) === yearMonth)
      assert(deserializeField(name) === yearMonth)
    }

    Seq(
      "interval day",
      "interval hour",
      "interval minute",
      "interval second",
      "interval day to hour",
      "interval day to minute",
      "interval day to second",
      "interval hour to minute",
      "interval hour to second",
      "interval minute to second").foreach { name =>
      assert(BasePrimitiveType.createPrimitive(name) === dayTime)
      assert(deserializeField(name) === dayTime)
    }

    assert(DataTypeJsonSerDe.serializeDataType(yearMonth) === "\"interval year to month\"")
    assert(DataTypeJsonSerDe.serializeDataType(dayTime) === "\"interval day to second\"")
    assert(BasePrimitiveType.getAllPrimitiveTypes.asScala.count(_ == yearMonth) === 1)
    assert(BasePrimitiveType.getAllPrimitiveTypes.asScala.count(_ == dayTime) === 1)
  }

  test("interval literals and generic data preserve signed physical values") {
    Seq(Int.MinValue, -13, 0, Int.MaxValue).foreach { months =>
      val literal = Literal.ofIntervalYearMonth(months)
      assert(literal.getDataType === yearMonth)
      assert(literal.getValue === Int.box(months))
    }
    Seq(Long.MinValue, -86400000000L, 0L, Long.MaxValue).foreach { micros =>
      val literal = Literal.ofIntervalDayTime(micros)
      assert(literal.getDataType === dayTime)
      assert(literal.getValue === Long.box(micros))
    }

    val schema = new StructType().add("months", yearMonth).add("micros", dayTime)
    val row = GenericRow.fromValues(schema, Seq(Int.box(-13), Long.box(86400000000L)).asJava)
    assert(row.getIntervalYearMonth(0) === -13)
    assert(row.getIntervalDayTime(1) === 86400000000L)
    intercept[UnsupportedOperationException](row.getInt(0))
    intercept[UnsupportedOperationException](row.getLong(1))

    val overrides = new util.HashMap[Integer, Object]()
    overrides.put(Int.box(0), Int.box(24))
    overrides.put(Int.box(1), Long.box(-1L))
    val delegated = new DelegateRow(row, overrides)
    assert(delegated.getIntervalYearMonth(0) === 24)
    assert(delegated.getIntervalDayTime(1) === -1L)

    val structs = new GenericColumnVector(util.Arrays.asList(row, null), schema)
    val structRow = StructRow.fromStructVector(structs, 0)
    assert(structRow.getIntervalYearMonth(0) === -13)
    assert(structRow.getIntervalDayTime(1) === 86400000000L)

    val months = structs.getChild(0)
    val micros = structs.getChild(1)
    assert(months.getIntervalYearMonth(0) === -13)
    assert(micros.getIntervalDayTime(0) === 86400000000L)
    assert(months.isNullAt(1))
    assert(micros.isNullAt(1))
    assert(VectorUtils.getValueAsObject(months, yearMonth, 0) === Int.box(-13))
    assert(VectorUtils.getValueAsObject(micros, dayTime, 0) === Long.box(86400000000L))
    intercept[IllegalArgumentException](months.getInt(0))
    intercept[IllegalArgumentException](micros.getLong(0))
  }

  test("intervals are rejected from persisted schemas at every nesting position") {
    Seq[DataType](yearMonth, dayTime).foreach { intervalType =>
      val schemas = Seq(
        new StructType().add("value", intervalType),
        new StructType().add("nested", new StructType().add("value", intervalType)),
        new StructType().add("array", new ArrayType(intervalType, true)),
        new StructType().add("mapKey", new MapType(intervalType, StringType.STRING, true)),
        new StructType().add("mapValue", new MapType(StringType.STRING, intervalType, true)))

      schemas.foreach { schema =>
        val error = intercept[KernelException] {
          SchemaUtils.validateSchema(schema, false, false, false)
        }
        assert(error.getMessage.contains(s"writing data of type: $intervalType"))
      }
    }
  }

  private def deserializeField(typeName: String): DataType = {
    val json =
      s"""{
         |"type":"struct",
         |"fields":[{"name":"value","type":"$typeName","nullable":true,"metadata":{}}]
         |}""".stripMargin
    DataTypeJsonSerDe.deserializeStructType(json).at(0).getDataType
  }
}
