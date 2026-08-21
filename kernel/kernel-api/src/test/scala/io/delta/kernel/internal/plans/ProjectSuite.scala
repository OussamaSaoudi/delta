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
package io.delta.kernel.internal.plans

import java.util

import scala.jdk.CollectionConverters._

import io.delta.kernel.expressions.{Column, Expression, Literal, StructExpression}
import io.delta.kernel.types.{LongType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll, Table}

class ProjectSuite extends AnyFunSuite {
  private val inputSchema = new StructType()
    .add("id", LongType.LONG, false)
    .add("name", StringType.STRING)

  test("Project declares logical names while preserving expression order") {
    val expression = new StructExpression(Seq[Expression](
      new Column("id"),
      new Column("name")).asJava)
    val output = new StructType()
      .add("identifier", LongType.LONG, false)
      .add("label", StringType.STRING)
    val plan = PlanBuilder.values(inputSchema, util.Collections.emptyList())
      .project(expression, output)
      .build()
    val project = plan.getNodes.get(1).getOperator.asInstanceOf[Project]

    assert(plan.getOutputSchema === output)
    assert(project.getExpression eq expression)
    assert(project.getSchema eq output)
  }

  test("Project delegates expression result validation to the expression evaluator") {
    val expression = Literal.ofLong(1)
    val project = new Project(expression, inputSchema)

    assert(project.getOutputSchema(Seq(inputSchema).asJava) === inputSchema)
    assert(project.getExpression eq expression)
  }

  test("Project requires exactly one non-null input schema") {
    val project = new Project(Literal.ofLong(1), inputSchema)
    val cases = Table(
      "inputs",
      Seq.empty[StructType],
      Seq(inputSchema, inputSchema))

    forAll(cases) { inputs =>
      val error = intercept[IllegalArgumentException] {
        project.getOutputSchema(inputs.asJava)
      }
      assert(error.getMessage.contains(s"requires one input, got ${inputs.size}"))
    }
    assertThrows[NullPointerException] {
      project.getOutputSchema(Seq(null.asInstanceOf[StructType]).asJava)
    }
  }

  test("Project requires a non-null expression and schema") {
    assertThrows[NullPointerException](new Project(null, inputSchema))
    assertThrows[NullPointerException](new Project(Literal.ofLong(1), null))
  }
}
