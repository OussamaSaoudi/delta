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

import scala.jdk.CollectionConverters._

import io.delta.kernel.expressions.{Column, Literal, Predicate}
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll, Table}

class RelationalPlanSuite extends AnyFunSuite {
  private val probeNested = new StructType().add("key", LongType.LONG, false)
  private val buildNested = new StructType().add("key", LongType.LONG, true)
  private val probeSchema = new StructType()
    .add("id", LongType.LONG, false)
    .add("nested", probeNested, false)
    .add("payload", StringType.STRING)
  private val buildSchema = new StructType()
    .add("key", LongType.LONG)
    .add("nested", buildNested, false)

  private def column(parts: String*): Column = new Column(parts.toArray)

  private def values(schema: StructType): Values =
    new Values(schema, util.Collections.emptyList())

  private def node(operator: Operator, inputs: Int*): PlanNode =
    new PlanNode(operator, inputs.map(IntegerJ.valueOf).asJava)

  test("Filter preserves its input schema and predicate reference") {
    val predicate = new Predicate(
      "AND",
      new Predicate(">", column("id"), Literal.ofLong(1)),
      new Predicate("IS_NOT_NULL", column("nested", "key")))
    val filter = new Filter(predicate)
    val plan = new Plan(Seq(node(values(probeSchema)), node(filter, 0)).asJava)

    assert(plan.getOutputSchema === probeSchema)
    assert(filter.getPredicate eq predicate)
  }

  test("Filter rejects missing references anywhere in its expression tree") {
    val cases = Table(
      ("column", "message"),
      (column("missing"), "Filter predicate column"),
      (column("nested", "missing"), "Filter predicate column"),
      (column("payload", "nested"), "Filter predicate column"),
      (column("ID"), "Filter predicate column"),
      (column(), "Filter predicate column"))

    forAll(cases) { (invalid, message) =>
      val predicate = new Predicate(
        "AND",
        new Predicate("IS_NOT_NULL", column("id")),
        new Predicate("IS_NOT_NULL", invalid))
      val error = intercept[IllegalArgumentException] {
        new Plan(Seq(node(values(probeSchema)), node(new Filter(predicate), 0)).asJava)
      }
      assert(error.getMessage.contains(message))
    }
  }

  test("SemiJoin and anti-join preserve probe schema and key order") {
    val modes = Table("inverted", false, true)
    forAll(modes) { inverted =>
      val join = new SemiJoin(
        inverted,
        Seq(column("id"), column("nested", "key")).asJava,
        Seq(column("key"), column("nested", "key")).asJava)
      val plan = new Plan(Seq(
        node(values(probeSchema)),
        node(values(buildSchema)),
        node(join, 0, 1)).asJava)

      assert(plan.getOutputSchema === probeSchema)
      assert(join.isInverted === inverted)
      assert(join.getProbeKeys.asScala === Seq(column("id"), column("nested", "key")))
      assert(join.getBuildKeys.asScala === Seq(column("key"), column("nested", "key")))
    }
  }

  test("SemiJoin accepts matching key types with different field nullability") {
    val join = new SemiJoin(
      false,
      Seq(column("id")).asJava,
      Seq(column("key")).asJava)

    assert(join.getOutputSchema(Seq(probeSchema, buildSchema).asJava) === probeSchema)
  }

  test("SemiJoin defensively copies and exposes immutable key lists") {
    val probeKeys = new util.ArrayList[Column](Seq(column("id")).asJava)
    val buildKeys = new util.ArrayList[Column](Seq(column("key")).asJava)
    val join = new SemiJoin(false, probeKeys, buildKeys)
    probeKeys.clear()
    buildKeys.clear()

    assert(join.getProbeKeys.asScala === Seq(column("id")))
    assert(join.getBuildKeys.asScala === Seq(column("key")))
    assertThrows[UnsupportedOperationException](join.getProbeKeys.clear())
    assertThrows[UnsupportedOperationException](join.getBuildKeys.clear())
  }

  test("SemiJoin rejects empty, unequal, and null keys") {
    val invalidKeys = Table(
      ("probe", "build", "message"),
      (Seq.empty[Column], Seq.empty[Column], "at least one key"),
      (Seq(column("id")), Seq(column("key"), column("key")), "1 probe key(s)"),
      (Seq(null.asInstanceOf[Column]), Seq(column("key")), "probe key is null"),
      (Seq(column("id")), Seq(null.asInstanceOf[Column]), "build key is null"))

    forAll(invalidKeys) { (probe, build, message) =>
      val error = intercept[RuntimeException] {
        new SemiJoin(false, probe.asJava, build.asJava)
      }
      assert(error.getMessage.contains(message))
    }
  }

  test("SemiJoin resolves every key against its corresponding input") {
    val cases = Table(
      ("probe", "build", "message"),
      (column("missing"), column("key"), "SemiJoin probe key column"),
      (column("id"), column("missing"), "SemiJoin build key column"),
      (column("nested", "missing"), column("key"), "SemiJoin probe key column"),
      (column("id"), column("key", "nested"), "SemiJoin build key column"),
      (column("ID"), column("key"), "SemiJoin probe key column"))

    forAll(cases) { (probe, build, message) =>
      val join = new SemiJoin(false, Seq(probe).asJava, Seq(build).asJava)
      val error = intercept[IllegalArgumentException] {
        join.getOutputSchema(Seq(probeSchema, buildSchema).asJava)
      }
      assert(error.getMessage.contains(message))
    }
  }

  test("SemiJoin requires exact key data types") {
    val primitiveMismatch = new StructType().add("key", StringType.STRING)
    val nestedProbe = new StructType().add(
      "key",
      new StructType().add("value", LongType.LONG, false))
    val nestedBuild = new StructType().add(
      "key",
      new StructType().add("value", LongType.LONG, true))
    val cases = Table(
      ("probe", "build"),
      (probeSchema, primitiveMismatch),
      (nestedProbe, nestedBuild))

    forAll(cases) { (probe, build) =>
      val probeKey = if (probe eq probeSchema) column("id") else column("key")
      val join = new SemiJoin(false, Seq(probeKey).asJava, Seq(column("key")).asJava)
      val error = intercept[IllegalArgumentException] {
        join.getOutputSchema(Seq(probe, build).asJava)
      }
      assert(error.getMessage.contains("has probe type"))
    }
  }

  test("relational operators enforce their input counts") {
    val filter = new Filter(new Predicate("IS_NOT_NULL", column("id")))
    val join = new SemiJoin(false, Seq(column("id")).asJava, Seq(column("key")).asJava)
    val cases = Table(
      ("operator", "inputs", "message"),
      (filter: Operator, Seq.empty[StructType], "Filter requires one input, got 0"),
      (filter: Operator, Seq(probeSchema, probeSchema), "Filter requires one input, got 2"),
      (join: Operator, Seq(probeSchema), "SemiJoin requires probe and build inputs, got 1"),
      (
        join: Operator,
        Seq(probeSchema, buildSchema, buildSchema),
        "SemiJoin requires probe and build inputs, got 3"))

    forAll(cases) { (operator, inputs, message) =>
      val error = intercept[IllegalArgumentException] {
        operator.getOutputSchema(inputs.asJava)
      }
      assert(error.getMessage.contains(message))
    }
  }
}
