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

import java.lang.{Boolean => BooleanJ, Integer => IntegerJ}
import java.util
import java.util.Optional

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.expressions._
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.plans.{Filter, PlanNode, Project, Values}
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class ProjectExecutorSuite extends AnyFunSuite with PlanExecutionSuiteBase {
  private val intSchema = new StructType()
    .add("a", IntegerType.INTEGER, true)
    .add("b", IntegerType.INTEGER, false)

  private def row(schema: StructType, values: AnyRef*): Row =
    GenericRow.fromValues(schema, values.asJava)

  private def values(schema: StructType, rows: Row*): Values =
    new Values(schema, rows.asJava)

  private def denseProject(
      input: PlanNode,
      expressions: Seq[Expression],
      schema: StructType): Project =
    new Project(input, new StructExpression(expressions.asJava), schema)

  private def identityProject(input: PlanNode, schema: StructType): Project =
    denseProject(input, schema.fields().asScala.map(f => new Column(f.getName)).toSeq, schema)

  test("dense projection evaluates expressions and uses declared field names") {
    val nestedSchema = new StructType().add("value", IntegerType.INTEGER, false)
    val outputSchema = new StructType()
      .add("filled", IntegerType.INTEGER, false)
      .add("incremented", IntegerType.INTEGER, false)
      .add("nested", nestedSchema, false)
    val coalesce = new ScalarExpression(
      "COALESCE",
      util.Arrays.asList(new Column("a"), Literal.ofInt(10)))
    val add = new ScalarExpression(
      "ADD",
      util.Arrays.asList(new Column("b"), Literal.ofInt(1)))
    val nested = new StructExpression(Seq[Expression](new Column("b")).asJava)
    val input = values(
      intSchema,
      row(intSchema, null, IntegerJ.valueOf(3)),
      row(intSchema, IntegerJ.valueOf(2), IntegerJ.valueOf(4)))

    checkRows(
      denseProject(input, Seq(coalesce, add, nested), outputSchema),
      Seq(
        row(
          outputSchema,
          IntegerJ.valueOf(10),
          IntegerJ.valueOf(4),
          row(nestedSchema, IntegerJ.valueOf(3))),
        row(
          outputSchema,
          IntegerJ.valueOf(2),
          IntegerJ.valueOf(5),
          row(nestedSchema, IntegerJ.valueOf(4)))))
  }

  test("struct patch preserves Rust field ordering for passthrough, replace, and drop") {
    val inputSchema = new StructType()
      .add("a", IntegerType.INTEGER, false)
      .add("b", IntegerType.INTEGER, false)
      .add("c", IntegerType.INTEGER, false)
    val transforms = new util.LinkedHashMap[String, StructPatch.FieldTransform]()
    transforms.put(
      "b",
      new StructPatch.FieldTransform(Seq[Expression](Literal.ofInt(9)).asJava, true, false))
    transforms.put(
      "c",
      new StructPatch.FieldTransform(
        util.Collections.emptyList[Expression](),
        true,
        false))
    val patch = new StructPatch(
      Optional.empty(),
      transforms,
      Seq[Expression](Literal.ofInt(0)).asJava,
      Seq[Expression](Literal.ofInt(99)).asJava)
    val outputSchema = new StructType()
      .add("pre", IntegerType.INTEGER, false)
      .add("a", IntegerType.INTEGER, false)
      .add("replaced_b", IntegerType.INTEGER, false)
      .add("tail", IntegerType.INTEGER, false)
    val input = values(
      inputSchema,
      row(inputSchema, IntegerJ.valueOf(1), IntegerJ.valueOf(10), IntegerJ.valueOf(100)),
      row(inputSchema, IntegerJ.valueOf(2), IntegerJ.valueOf(20), IntegerJ.valueOf(200)))

    checkRows(
      new Project(input, patch, outputSchema),
      Seq(
        row(
          outputSchema,
          IntegerJ.valueOf(0),
          IntegerJ.valueOf(1),
          IntegerJ.valueOf(9),
          IntegerJ.valueOf(99)),
        row(
          outputSchema,
          IntegerJ.valueOf(0),
          IntegerJ.valueOf(2),
          IntegerJ.valueOf(9),
          IntegerJ.valueOf(99))))
  }

  test("projection preserves a narrowed selection") {
    val input = values(
      intSchema,
      row(intSchema, IntegerJ.valueOf(1), IntegerJ.valueOf(10)),
      row(intSchema, IntegerJ.valueOf(2), IntegerJ.valueOf(20)),
      row(intSchema, IntegerJ.valueOf(3), IntegerJ.valueOf(30)))
    val selected = new Filter(
      input,
      new Predicate(">", new Column("a"), Literal.ofInt(1)))

    checkRows(
      identityProject(selected, intSchema),
      Seq(
        row(intSchema, IntegerJ.valueOf(2), IntegerJ.valueOf(20)),
        row(intSchema, IntegerJ.valueOf(3), IntegerJ.valueOf(30))))
  }

  test("projection supports a zero-column result") {
    val outputSchema = new StructType()
    val input = values(intSchema, row(intSchema, IntegerJ.valueOf(1), IntegerJ.valueOf(10)))

    checkRows(
      denseProject(input, Seq.empty, outputSchema),
      Seq(row(outputSchema)))
  }

  test("invalid evaluator setup fails when execution is wired") {
    val badOutput = new StructType().add("bad", StringType.STRING)
    val input = values(intSchema, row(intSchema, IntegerJ.valueOf(1), IntegerJ.valueOf(10)))
    val project = denseProject(input, Seq(new Column("a")), badOutput)

    intercept[UnsupportedOperationException] {
      checkRows(project, Seq.empty)
    }
  }

  test("nullable struct roots mask every projected field") {
    val denseInputSchema = new StructType()
      .add("value", IntegerType.INTEGER, false)
      .add("keep", BooleanType.BOOLEAN, true)
    val denseOutputSchema = new StructType()
      .add("value", IntegerType.INTEGER, false)
      .add("constant", IntegerType.INTEGER, false)
    val denseExpression = new StructExpression(
      Seq[Expression](new Column("value"), Literal.ofInt(9)).asJava,
      new Column("keep"))
    val denseInput = values(
      denseInputSchema,
      row(denseInputSchema, IntegerJ.valueOf(1), BooleanJ.TRUE),
      row(denseInputSchema, IntegerJ.valueOf(2), BooleanJ.FALSE),
      row(denseInputSchema, IntegerJ.valueOf(3), null))

    checkRows(
      new Project(denseInput, denseExpression, denseOutputSchema),
      Seq(
        row(denseOutputSchema, IntegerJ.valueOf(1), IntegerJ.valueOf(9)),
        row(denseOutputSchema, null, null),
        row(denseOutputSchema, null, null)))

    val innerType = new StructType().add("x", IntegerType.INTEGER, false)
    val outerType = new StructType().add("inner", innerType, false)
    val nestedInputSchema = new StructType().add("outer", outerType, true)
    val nestedInput = values(
      nestedInputSchema,
      row(
        nestedInputSchema,
        row(outerType, row(innerType, IntegerJ.valueOf(7)))),
      row(nestedInputSchema, null))
    val nestedPatch = new StructPatch(
      Optional.of(new Column(Array("outer", "inner"))),
      util.Collections.emptyMap[String, StructPatch.FieldTransform](),
      util.Collections.emptyList[Expression](),
      util.Collections.emptyList[Expression]())

    checkRows(
      new Project(nestedInput, nestedPatch, innerType),
      Seq(row(innerType, IntegerJ.valueOf(7)), row(innerType, null)))
  }
}
