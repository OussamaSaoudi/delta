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
package io.delta.kernel.defaults.internal.plans

import java.lang.{Long => LongJ}
import java.util

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.plans.PlanBuilder
import io.delta.kernel.types.{LongType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class ValuesPlanSuite extends AnyFunSuite with PlanExecutionSuiteBase {
  private val schema = new StructType()
    .add("id", LongType.LONG, false)
    .add("label", StringType.STRING)

  private def row(id: Long, label: String): Row =
    GenericRow.fromValues(schema, Seq(LongJ.valueOf(id), label).asJava)

  test("execute Values") {
    val expected = Seq(row(1, "one"), row(2, null))
    checkRows(PlanBuilder.values(schema, expected.asJava), expected)
  }

  test("execute empty Values") {
    checkRows(PlanBuilder.values(schema, util.Collections.emptyList[Row]()), Seq.empty)
  }
}
