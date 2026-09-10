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
import io.delta.kernel.expressions.{Column, Literal, Predicate}
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.plans.{Filter, Values}
import io.delta.kernel.types.{LongType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class FilterPlanSuite extends AnyFunSuite with PlanExecutionSuiteBase {
  private val schema = new StructType()
    .add("id", LongType.LONG)
    .add("label", StringType.STRING)

  private def column(name: String): Column = new Column(name)

  private def row(id: java.lang.Long, label: String): Row =
    GenericRow.fromValues(schema, Seq(id, label).asJava)

  test("evaluate predicates with null semantics") {
    val input = Seq(
      row(LongJ.valueOf(1), "one"),
      row(null, "unknown"),
      row(LongJ.valueOf(3), "three"),
      row(LongJ.valueOf(5), "five"))
    val predicate = new Predicate(">", column("id"), Literal.ofLong(2))

    checkRows(
      new Filter(new Values(schema, input.asJava), predicate),
      Seq(row(LongJ.valueOf(3), "three"), row(LongJ.valueOf(5), "five")))
  }

  test("compose filters through selection vectors") {
    val input = Seq(
      row(LongJ.valueOf(1), "one"),
      row(LongJ.valueOf(3), "three"),
      row(LongJ.valueOf(5), "five"))
    val greaterThanOne = new Predicate(">", column("id"), Literal.ofLong(1))
    val lessThanFive = new Predicate("<", column("id"), Literal.ofLong(5))
    val source = new Values(schema, input.asJava)

    checkRows(
      new Filter(new Filter(source, greaterThanOne), lessThanFive),
      Seq(row(LongJ.valueOf(3), "three")))
  }

  test("reject unresolved predicate columns while building") {
    val error = intercept[IllegalArgumentException] {
      new Filter(
        new Values(schema, java.util.Collections.emptyList[Row]()),
        new Predicate("IS_NOT_NULL", column("missing")))
    }

    assert(error.getMessage.contains("Filter predicate"))
    assert(error.getMessage.contains("missing"))
  }
}
