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

import scala.collection.JavaConverters._

import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class IntervalVectorSuite extends AnyFunSuite {
  private val yearMonth = IntervalYearMonthType.INTERVAL_YEAR_MONTH
  private val dayTime = IntervalDayTimeType.INTERVAL_DAY_TIME

  test("physical interval vectors expose only their dedicated accessors") {
    val months = new DefaultIntVector(
      yearMonth,
      3,
      Optional.of(Array(false, true, false)),
      Array(Int.MinValue, 0, Int.MaxValue))
    assert(months.getIntervalYearMonth(0) === Int.MinValue)
    assert(months.isNullAt(1))
    assert(months.getIntervalYearMonth(2) === Int.MaxValue)
    intercept[IllegalArgumentException](months.getInt(0))

    val micros = new DefaultLongVector(
      dayTime,
      3,
      Optional.of(Array(false, true, false)),
      Array(Long.MinValue, 0L, Long.MaxValue))
    assert(micros.getIntervalDayTime(0) === Long.MinValue)
    assert(micros.isNullAt(1))
    assert(micros.getIntervalDayTime(2) === Long.MaxValue)
    intercept[IllegalArgumentException](micros.getLong(0))

    val monthsView = new DefaultViewVector(months, 1, 3)
    val microsView = new DefaultViewVector(micros, 1, 3)
    assert(monthsView.isNullAt(0))
    assert(monthsView.getIntervalYearMonth(1) === Int.MaxValue)
    assert(microsView.isNullAt(0))
    assert(microsView.getIntervalDayTime(1) === Long.MaxValue)
  }

  test("generic and subfield vectors preserve interval types") {
    val months = DefaultGenericVector.fromArray(
      yearMonth,
      Array[AnyRef](Int.box(-13), null, Int.box(Int.MaxValue)))
    assert(months.getIntervalYearMonth(0) === -13)
    assert(months.isNullAt(1))
    assert(months.getIntervalYearMonth(2) === Int.MaxValue)
    intercept[UnsupportedOperationException](months.getInt(0))

    val schema = new StructType().add("months", yearMonth).add("micros", dayTime)
    val row = GenericRow.fromValues(schema, Seq(Int.box(30), Long.box(-5L)).asJava)
    val structs = DefaultGenericVector.fromArray(schema, Array[AnyRef](row))
    assert(structs.getChild(0).getIntervalYearMonth(0) === 30)
    assert(structs.getChild(1).getIntervalDayTime(0) === -5L)
  }
}
