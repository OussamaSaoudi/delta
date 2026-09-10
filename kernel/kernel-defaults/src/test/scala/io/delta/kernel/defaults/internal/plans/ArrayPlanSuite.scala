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

class ArrayPlanSuite extends AnyFunSuite with PlanExecutionSuiteBase {
  private val inputSchema = new StructType()
    .add("left", IntegerType.INTEGER)
    .add("right", IntegerType.INTEGER)
  private val arrayType = new ArrayType(IntegerType.INTEGER, true)
  private val outputSchema = new StructType().add("values", arrayType, false)

  private def row(schema: StructType, values: AnyRef*): Row =
    GenericRow.fromValues(schema, values.asJava)

  test("execute ARRAY projection without converting Kernel batches") {
    val input = Seq(
      row(inputSchema, IntegerJ.valueOf(1), null),
      row(inputSchema, IntegerJ.valueOf(2), IntegerJ.valueOf(20)))
    val plan = new Project(
      new Values(inputSchema, input.asJava),
      struct(array(col("left"), col("right"), int(42))),
      outputSchema)

    def expected(values: Integer*): Row = row(
      outputSchema,
      VectorUtils.buildArrayValue(values.asJava, IntegerType.INTEGER))

    checkRows(
      plan,
      Seq(
        row(
          outputSchema,
          VectorUtils.buildArrayValue(
            Seq[IntegerJ](IntegerJ.valueOf(1), null, IntegerJ.valueOf(42)).asJava,
            IntegerType.INTEGER)),
        expected(2, 20, 42)))
  }

  test("reject invalid ARRAY shapes while binding the plan") {
    val input = new Values(inputSchema, Seq.empty[Row].asJava)

    val empty = intercept[UnsupportedOperationException] {
      checkRows(
        new Project(
          input,
          struct(array()),
          new StructType().add("values", arrayType)),
        Seq.empty)
    }
    assert(empty.getMessage.contains("requires at least one element"))

    val wrongOutput = intercept[UnsupportedOperationException] {
      checkRows(
        new Project(
          input,
          struct(array(int(1))),
          new StructType().add("values", StringType.STRING)),
        Seq.empty)
    }
    assert(wrongOutput.getMessage.contains("requires an ArrayType result"))
  }
}
