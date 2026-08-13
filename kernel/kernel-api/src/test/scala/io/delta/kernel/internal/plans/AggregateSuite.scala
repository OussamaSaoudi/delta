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
import scala.jdk.OptionConverters._

import io.delta.kernel.expressions.Column
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll, Table}

class AggregateSuite extends AnyFunSuite {
  private val groupMetadata = FieldMetadata.builder().putString("source", "group").build()
  private val valueMetadata = FieldMetadata.builder().putString("source", "value").build()
  private val groupField = new StructField("group", StringType.STRING, false, groupMetadata)
  private val valueField = new StructField("value", LongType.LONG, false, valueMetadata)
  private val keyField = new StructField("key", IntegerType.INTEGER, true)
  private val inputSchema = new StructType(Seq(groupField, valueField, keyField).asJava)

  private def column(parts: String*): Column = new Column(parts.toArray)

  private def node(operator: Operator, inputs: Int*): PlanNode =
    new PlanNode(operator, inputs.map(IntegerJ.valueOf).asJava)

  test("all aggregate variants derive a nullable value field without metadata") {
    val variants = Table(
      ("function", "aggregate", "key"),
      (Agg.Function.MIN, Agg.min(column("value")), None),
      (Agg.Function.MAX, Agg.max(column("value")), None),
      (
        Agg.Function.MIN_NON_NULL_BY,
        Agg.minNonNullBy(column("value"), column("key")),
        Some(column("key"))),
      (
        Agg.Function.MAX_NON_NULL_BY,
        Agg.maxNonNullBy(column("value"), column("key")),
        Some(column("key"))))

    forAll(variants) { (function, agg, expectedKey) =>
      val operator = Aggregate.ungrouped(inputSchema).aggregate(agg).build()
      val output = operator.getSchema.at(0)

      assert(agg.getFunction === function)
      assert(agg.getValue === column("value"))
      assert(agg.getKey.asScala === expectedKey)
      assert(output.getName === "value")
      assert(output.getDataType === LongType.LONG)
      assert(output.isNullable)
      assert(output.getMetadata === FieldMetadata.empty())
    }
  }

  test("group fields pass through before aggregates with type, nullability, and metadata") {
    val operator = Aggregate.groupBy(inputSchema, Seq(column("group")).asJava)
      .maxNonNullBy(column("value"), column("key"))
      .build()

    assert(operator.getSchema.fields.asScala === Seq(
      groupField,
      new StructField("value", LongType.LONG, true)))
    assert(operator.getSchema.at(0) eq groupField)
  }

  test("nested value names and explicit aliases determine aggregate output names") {
    val nestedField = new StructField("leaf", LongType.LONG, true, valueMetadata)
    val nestedSchema = new StructType().add(
      "outer",
      new StructType(Seq(nestedField).asJava),
      true)

    val operator = Aggregate.ungrouped(nestedSchema)
      .min(column("outer", "leaf"))
      .aggregateAs(Agg.max(column("outer", "leaf")), "greatest")
      .aggregateAs(Agg.max(column("outer")), "whole")
      .build()

    assert(operator.getSchema.fieldNames.asScala === Seq("leaf", "greatest", "whole"))
    assert(operator.getSchema.fields.asScala.forall(_.getMetadata === FieldMetadata.empty()))
    val nestedOutput = operator.getSchema.at(2).getDataType.asInstanceOf[StructType]
    assert(nestedOutput.at(0).getMetadata === FieldMetadata.empty())
  }

  test("duplicate output names are rejected case-insensitively") {
    val builders = Table(
      "builder",
      (() =>
        Aggregate.ungrouped(inputSchema)
          .min(column("value"))
          .max(column("value"))): (() => AggregateBuilder),
      (() =>
        Aggregate.groupBy(inputSchema, Seq(column("group")).asJava)
          .aggregateAs(Agg.max(column("value")), "GROUP")): (() => AggregateBuilder),
      (() =>
        Aggregate.ungrouped(inputSchema)
          .aggregateAs(Agg.min(column("value")), "result")
          .aggregateAs(Agg.max(column("value")), "RESULT")): (() => AggregateBuilder))

    forAll(builders) { builder =>
      val error = intercept[IllegalArgumentException] { builder().build() }
      assert(error.getMessage.contains("Duplicate aggregate output name"))
    }
  }

  test("construction rejects every unresolved operand") {
    val builders = Table(
      ("builder", "context"),
      (
        () =>
          Aggregate.groupBy(inputSchema, Seq(column("missing")).asJava)
            .max(column("value")),
        "group key"),
      (() => Aggregate.ungrouped(inputSchema).min(column("missing")), "value"),
      (
        () =>
          Aggregate.ungrouped(inputSchema)
            .minNonNullBy(column("missing"), column("key")),
        "value"),
      (
        () =>
          Aggregate.ungrouped(inputSchema)
            .maxNonNullBy(column("value"), column("missing")),
        "key"))

    forAll(builders) { (builder, context) =>
      val error = intercept[IllegalArgumentException] { builder().build() }
      assert(error.getMessage.contains(s"Aggregate $context column"))
      assert(error.getMessage.contains("missing"))
    }
  }

  test("Plan validates Aggregate input count and actual child schema") {
    val operator = Aggregate.groupBy(inputSchema, Seq(column("group")).asJava)
      .max(column("value"))
      .build()
    val source = new Values(inputSchema, util.Collections.emptyList())
    val plan = new Plan(Seq(node(source), node(operator, 0)).asJava)
    assert(plan.getOutputSchema === operator.getSchema)

    val counts = Table("inputs", Seq.empty[StructType], Seq(inputSchema, inputSchema))
    forAll(counts) { inputs =>
      val error = intercept[IllegalArgumentException] {
        operator.getOutputSchema(inputs.asJava)
      }
      assert(error.getMessage.contains(s"requires one input, got ${inputs.size}"))
    }

    val incompatible = new StructType()
      .add(groupField)
      .add("value", StringType.STRING, false)
      .add(keyField)
    val mismatch = intercept[IllegalArgumentException] {
      operator.getOutputSchema(Seq(incompatible).asJava)
    }
    assert(mismatch.getMessage.contains("output schema does not match its input"))

    val missingValue = new StructType().add(groupField).add(keyField)
    val unresolved = intercept[IllegalArgumentException] {
      operator.getOutputSchema(Seq(missingValue).asJava)
    }
    assert(unresolved.getMessage.contains("Aggregate value column"))
  }

  test("aggregate payloads defensively copy and expose immutable collections") {
    val keys = new util.ArrayList[Column](Seq(column("group")).asJava)
    val builder = Aggregate.groupBy(inputSchema, keys).max(column("value"))
    keys.clear()
    val first = builder.build()
    builder.minNonNullBy(column("value"), column("key"))

    assert(first.getGroupBy.asScala === Seq(column("group")))
    assert(first.getAggs.size() === 1)
    assertThrows[UnsupportedOperationException](first.getGroupBy.clear())
    assertThrows[UnsupportedOperationException](first.getAggs.clear())
  }

  test("builders reject null elements and empty value paths") {
    assertThrows[NullPointerException] {
      Aggregate.groupBy(inputSchema, Seq(null.asInstanceOf[Column]).asJava)
    }
    assertThrows[NullPointerException] {
      Aggregate.ungrouped(inputSchema).aggregate(null)
    }
    assertThrows[NullPointerException] {
      Aggregate.ungrouped(inputSchema).aggregateAs(Agg.max(column("value")), null)
    }

    val error = intercept[IllegalArgumentException] {
      Aggregate.ungrouped(inputSchema).max(new Column(Array.empty[String])).build()
    }
    assert(error.getMessage.contains("absent from schema"))
  }
}
