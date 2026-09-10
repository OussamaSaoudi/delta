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
package io.delta.kernel.plans

import java.util

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.expressions.{Column, Expression, StructExpression}
import io.delta.kernel.types.{LongType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class ProjectSuite extends AnyFunSuite {
  private val inputSchema = new StructType()
    .add("id", LongType.LONG, false)
    .add("name", StringType.STRING)

  test("Project contains one struct-valued expression") {
    val input = new Values(inputSchema, util.Collections.emptyList[Row]())
    val expression = new StructExpression(Seq[Expression](
      new Column("id"),
      new Column("name")).asJava)
    val output = new StructType()
      .add("identifier", LongType.LONG, false)
      .add("label", StringType.STRING)
    val project = new Project(input, expression, output)

    assert(project.input() eq input)
    assert(project.rowExpression() eq expression)
    assert(project.outputSchema() === output)
    assert(project.children().asScala === Seq(input))
  }

  test("Project validates expression references") {
    val input = new Values(inputSchema, util.Collections.emptyList[Row]())
    assertThrows[IllegalArgumentException] {
      new Project(
        input,
        new StructExpression(Seq[Expression](new Column("missing")).asJava),
        new StructType().add("missing", LongType.LONG))
    }
  }
}
