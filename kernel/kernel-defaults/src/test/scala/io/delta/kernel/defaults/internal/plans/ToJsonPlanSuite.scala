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

import java.lang.{Integer => IntegerJ}

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.plans.{Project, Values}
import io.delta.kernel.types.{ArrayType, IntegerType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class ToJsonPlanSuite extends AnyFunSuite with PlanExecutionSuiteBase {
  private val nestedType = new StructType().add("id", IntegerType.INTEGER, false)
  private val valueType = new StructType()
    .add("missing", StringType.STRING, true)
    .add("values", new ArrayType(IntegerType.INTEGER, true), false)
    .add("nested", nestedType, false)
  private val inputSchema = new StructType().add("value", valueType, true)
  private val outputSchema = new StructType().add("json", StringType.STRING, true)

  private def row(schema: StructType, values: AnyRef*): Row =
    GenericRow.fromValues(schema, values.asJava)

  test("execute TO_JSON projection over native Kernel values") {
    val value = row(
      valueType,
      null,
      VectorUtils.buildArrayValue(
        Seq[IntegerJ](IntegerJ.valueOf(1), null, IntegerJ.valueOf(2)).asJava,
        IntegerType.INTEGER),
      row(nestedType, IntegerJ.valueOf(7)))
    val input = Seq(row(inputSchema, value), row(inputSchema, null))
    val plan = new Project(
      new Values(inputSchema, input.asJava),
      struct(toJson(col("value"))),
      outputSchema)

    checkRows(
      plan,
      Seq(
        row(outputSchema, "{\"values\":[1,null,2],\"nested\":{\"id\":7}}"),
        row(outputSchema, null)))
  }

  test("reject TO_JSON over non-struct input while binding the plan") {
    val schema = new StructType().add("value", IntegerType.INTEGER)
    val input = new Values(schema, Seq.empty[Row].asJava)
    val plan = new Project(input, struct(toJson(col("value"))), outputSchema)

    val error = intercept[UnsupportedOperationException] {
      checkRows(plan, Seq.empty)
    }
    assert(error.getMessage.contains("requires exactly one struct input"))
  }
}
