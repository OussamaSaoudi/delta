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

import java.lang.{Integer => IntegerJ}
import java.util
import java.util.Optional

import scala.jdk.CollectionConverters._

import io.delta.kernel.expressions._
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll, Table}

class ProjectSuite extends AnyFunSuite {
  private val nestedSchema = new StructType()
    .add("x", LongType.LONG, false)
    .add("y", StringType.STRING)
  private val inputSchema = new StructType()
    .add("id", LongType.LONG, false)
    .add("name", StringType.STRING)
    .add("nested", nestedSchema)

  private def column(parts: String*): Column = new Column(parts.toArray)

  private def node(operator: Operator, inputs: Int*): PlanNode =
    new PlanNode(operator, inputs.map(IntegerJ.valueOf).asJava)

  private def source: Values =
    new Values(inputSchema, util.Collections.emptyList())

  private def transform(
      replace: Boolean,
      expressions: Seq[Expression] = Seq.empty,
      optional: Boolean = false): StructPatch.FieldTransform =
    new StructPatch.FieldTransform(expressions.asJava, replace, optional)

  private def patch(
      transforms: Seq[(String, StructPatch.FieldTransform)] = Seq.empty,
      prepended: Seq[Expression] = Seq.empty,
      appended: Seq[Expression] = Seq.empty,
      inputPath: Option[Column] = None): StructPatch = {
    val fields = new util.LinkedHashMap[String, StructPatch.FieldTransform]()
    transforms.foreach { case (name, fieldTransform) => fields.put(name, fieldTransform) }
    new StructPatch(
      inputPath.map(Optional.of(_)).getOrElse(Optional.empty()),
      fields,
      prepended.asJava,
      appended.asJava)
  }

  test("dense Project declares logical names while preserving expression order") {
    val expression = new StructExpression(Seq[Expression](
      column("id"),
      column("nested", "y")).asJava)
    val output = new StructType()
      .add("identifier", LongType.LONG, false)
      .add("label", StringType.STRING)
    val project = new Project(expression, output)
    val plan = new Plan(Seq(node(source), node(project, 0)).asJava)

    assert(plan.getOutputSchema === output)
    assert(project.getExpression eq expression)
    assert(project.getSchema eq output)
  }

  test("Project validates and counts sparse patch fields in Rust emission order") {
    val expression = patch(
      transforms = Seq(
        "name" -> transform(replace = true, Seq(Literal.ofString("fixed"))),
        "nested" -> transform(replace = true)),
      prepended = Seq(Literal.ofLong(0)),
      appended = Seq(column("nested", "x")))
    val output = new StructType()
      .add("pre", LongType.LONG, false)
      .add("id", LongType.LONG, false)
      .add("name", StringType.STRING, false)
      .add("nested_x", LongType.LONG, false)

    assert(new Project(expression, output)
      .getOutputSchema(Seq(inputSchema).asJava) === output)
  }

  test("Project supports a patch rooted at a nested input struct") {
    val expression = patch(
      transforms = Seq(
        "x" -> transform(replace = false, Seq(Literal.ofLong(7))),
        "y" -> transform(replace = true)),
      inputPath = Some(column("nested")))
    val output = new StructType()
      .add("x", LongType.LONG, false)
      .add("after_x", LongType.LONG, false)

    assert(new Project(expression, output)
      .getOutputSchema(Seq(inputSchema).asJava) === output)
  }

  test("Project rejects unresolved references in every active expression position") {
    val missing = column("missing")
    val activeTransform = patch(Seq("id" -> transform(replace = true, Seq(missing))))
    val nestedPatch = patch(inputPath = Some(missing))
    val cases = Table(
      "expression",
      new StructExpression(Seq[Expression](missing).asJava),
      new StructExpression(
        Seq[Expression](column("id")).asJava,
        new Predicate("IS_NOT_NULL", missing)),
      patch(prepended = Seq(missing)),
      activeTransform,
      patch(appended = Seq(missing)),
      new StructExpression(Seq[Expression](nestedPatch).asJava))

    forAll(cases) { expression =>
      val output = new StructType().add("value", LongType.LONG)
      val error = intercept[IllegalArgumentException] {
        new Project(expression, output).getOutputSchema(Seq(inputSchema).asJava)
      }
      assert(error.getMessage.contains("Project"))
      assert(error.getMessage.contains("absent from schema"))
    }
  }

  test("patch field validation distinguishes required and optional missing anchors") {
    val required = patch(Seq("missing" -> transform(replace = true)))
    val error = intercept[IllegalArgumentException] {
      new Project(required, inputSchema).getOutputSchema(Seq(inputSchema).asJava)
    }
    assert(error.getMessage.contains("Required Project struct patch field"))

    val ignored = patch(Seq(
      "missing" -> transform(
        replace = true,
        expressions = Seq(column("also_missing")),
        optional = true)))
    assert(new Project(ignored, inputSchema)
      .getOutputSchema(Seq(inputSchema).asJava) === inputSchema)
  }

  test("Project rejects non-struct and absent patch input paths") {
    val paths = Table(
      ("path", "message"),
      (column("missing"), "absent from schema"),
      (column("id"), "must resolve to a struct"))

    forAll(paths) { (path, message) =>
      val error = intercept[IllegalArgumentException] {
        new Project(patch(inputPath = Some(path)), inputSchema)
          .getOutputSchema(Seq(inputSchema).asJava)
      }
      assert(error.getMessage.contains(message))
    }
  }

  test("Project enforces dense and sparse output arity") {
    val dense = new StructExpression(Seq[Expression](column("id"), column("name")).asJava)
    val sparse = patch()
    val cases = Table(
      ("expression", "fieldCount"),
      (dense: Expression, 1),
      (dense: Expression, 3),
      (sparse: Expression, 2),
      (sparse: Expression, 4))

    forAll(cases) { (expression, fieldCount) =>
      val fields = (0 until fieldCount)
        .map(index => new StructField(s"field_$index", LongType.LONG, true))
      val output = new StructType(fields.asJava)
      val error = intercept[IllegalArgumentException] {
        new Project(expression, output).getOutputSchema(Seq(inputSchema).asJava)
      }
      assert(error.getMessage.contains("fields, but output schema declares"))
    }
  }

  test("Project requires exactly one non-null input schema") {
    val expression = new StructExpression(Seq[Expression](column("id")).asJava)
    val output = new StructType().add("id", LongType.LONG, false)
    val project = new Project(expression, output)
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

  test("Project construction requires a struct expression and declared schema") {
    val invalidRoots = Table(
      "expression",
      Literal.ofLong(1): Expression,
      column("id"): Expression,
      new Predicate("IS_NOT_NULL", column("id")): Expression)

    forAll(invalidRoots) { expression =>
      val error = intercept[IllegalArgumentException] {
        new Project(expression, inputSchema)
      }
      assert(error.getMessage.contains("StructExpression or StructPatch"))
    }
    assertThrows[NullPointerException](new Project(null, inputSchema))
    assertThrows[NullPointerException](new Project(patch(), null))
  }
}
