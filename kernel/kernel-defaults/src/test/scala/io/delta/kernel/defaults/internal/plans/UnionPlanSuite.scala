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

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.plans.PlanBuilder
import io.delta.kernel.types.{LongType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class UnionPlanSuite extends AnyFunSuite with PlanExecutionSuiteBase {
  private val schema = new StructType()
    .add("id", LongType.LONG, false)
    .add("label", StringType.STRING)

  private def row(id: Long): Row =
    GenericRow.fromValues(schema, Seq(LongJ.valueOf(id), s"value-$id").asJava)

  private def values(rows: Row*): PlanBuilder = PlanBuilder.values(schema, rows.asJava)

  test("execute UnionAll") {
    val expected = Seq(row(1), row(2), row(3))
    val union = PlanBuilder.unionAll(Seq(
      values(expected(0), expected(1)),
      values(expected(2))).asJava)

    checkRows(union, expected)
  }

  test("execute a shared input") {
    val one = row(1)
    val source = values(one)

    checkRows(PlanBuilder.unionAll(Seq(source, source).asJava), Seq(one, one))
  }
}
