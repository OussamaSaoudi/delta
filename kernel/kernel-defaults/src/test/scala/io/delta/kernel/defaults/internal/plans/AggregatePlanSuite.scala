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

import java.lang.{Double => DoubleJ, Integer => IntegerJ}
import java.util

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.plans.{Agg, Aggregate, PlanBuilder}
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class AggregatePlanSuite extends AnyFunSuite with PlanExecutionSuiteBase {
  private def column(name: String): Column = new Column(name)

  private def row(schema: StructType, values: AnyRef*): Row =
    GenericRow.fromValues(schema, values.asJava)

  test("execute every ungrouped aggregate across input batches") {
    val schema = new StructType()
      .add("score", IntegerType.INTEGER)
      .add("value", StringType.STRING)
      .add("order", IntegerType.INTEGER)
    val first = Seq(
      row(schema, IntegerJ.valueOf(5), "five", IntegerJ.valueOf(5)),
      row(schema, null, "three", IntegerJ.valueOf(3)),
      row(schema, IntegerJ.valueOf(7), null, IntegerJ.valueOf(7)))
    val second = Seq(
      row(schema, IntegerJ.valueOf(1), "one", IntegerJ.valueOf(1)),
      row(schema, IntegerJ.valueOf(8), "eight", IntegerJ.valueOf(8)))
    val aggregate = Aggregate.ungrouped(schema)
      .aggregateAs(Agg.min(column("score")), "least")
      .aggregateAs(Agg.max(column("score")), "greatest")
      .aggregateAs(Agg.minNonNullBy(column("value"), column("order")), "earliest")
      .aggregateAs(Agg.maxNonNullBy(column("value"), column("order")), "latest")
      .build()
    val input = PlanBuilder.unionAll(Seq(
      PlanBuilder.values(schema, first.asJava),
      PlanBuilder.values(schema, second.asJava)).asJava)
    val expected = row(
      aggregate.getSchema,
      IntegerJ.valueOf(1),
      IntegerJ.valueOf(8),
      "one",
      "eight")

    assert(aggregate.getSchema.fieldNames.asScala ===
      Seq("least", "greatest", "earliest", "latest"))
    assert(aggregate.getSchema.fields.asScala.forall(_.isNullable))
    checkRows(input.aggregate(aggregate), Seq(expected))
  }

  test("empty input produces one null row") {
    val schema = new StructType().add("id", IntegerType.INTEGER)
    val aggregate = Aggregate.ungrouped(schema).min(column("id")).max(column("id")).build()
    val input = PlanBuilder.values(schema, util.Collections.emptyList[Row]())

    checkRows(input.aggregate(aggregate), Seq(row(aggregate.getSchema, null, null)))
  }

  test("group rows across batches with null-safe keys") {
    val schema = new StructType()
      .add("group", StringType.STRING)
      .add("score", IntegerType.INTEGER)
      .add("value", StringType.STRING)
      .add("order", IntegerType.INTEGER)
    val first = PlanBuilder.values(schema, Seq(
      row(schema, "a", IntegerJ.valueOf(5), "five", IntegerJ.valueOf(5)),
      row(schema, null, IntegerJ.valueOf(2), "two", IntegerJ.valueOf(2))).asJava)
    val second = PlanBuilder.values(schema, Seq(
      row(schema, "a", IntegerJ.valueOf(8), "eight", IntegerJ.valueOf(8)),
      row(schema, null, IntegerJ.valueOf(1), "one", IntegerJ.valueOf(1))).asJava)
    val plan = PlanBuilder.unionAll(Seq(first, second).asJava).aggregateBy(
      Seq(column("group")).asJava,
      _.aggregateAs(Agg.min(column("score")), "least")
        .aggregateAs(Agg.max(column("score")), "greatest")
        .aggregateAs(Agg.minNonNullBy(column("value"), column("order")), "earliest")
        .aggregateAs(Agg.maxNonNullBy(column("value"), column("order")), "latest"))
    val outputSchema = plan.getOutputSchema

    checkRowsUnordered(plan, Seq(
      row(outputSchema, "a", IntegerJ.valueOf(5), IntegerJ.valueOf(8), "five", "eight"),
      row(outputSchema, null, IntegerJ.valueOf(1), IntegerJ.valueOf(2), "one", "two")))
  }

  test("grouping normalizes signed zero and compares binary by contents") {
    val numberSchema = new StructType()
      .add("group", DoubleType.DOUBLE)
      .add("value", IntegerType.INTEGER)
    val numbers = Seq(
      row(numberSchema, DoubleJ.valueOf(-0.0d), IntegerJ.valueOf(1)),
      row(numberSchema, DoubleJ.valueOf(0.0d), IntegerJ.valueOf(2)))
    val numberPlan = PlanBuilder.values(numberSchema, numbers.asJava)
      .aggregateBy(Seq(column("group")).asJava, _.max(column("value")))
    val numberOutput = numberPlan.getOutputSchema

    checkRows(numberPlan, Seq(row(numberOutput, DoubleJ.valueOf(-0.0d), IntegerJ.valueOf(2))))

    val binarySchema = new StructType()
      .add("group", BinaryType.BINARY)
      .add("value", IntegerType.INTEGER)
    val binaries = Seq(
      row(binarySchema, Array[Byte](1, 2), IntegerJ.valueOf(1)),
      row(binarySchema, Array[Byte](1, 2), IntegerJ.valueOf(2)))
    val binaryPlan = PlanBuilder.values(binarySchema, binaries.asJava)
      .aggregateBy(Seq(column("group")).asJava, _.max(column("value")))
    val binaryOutput = binaryPlan.getOutputSchema

    checkRows(binaryPlan, Seq(row(binaryOutput, Array[Byte](1, 2), IntegerJ.valueOf(2))))
  }

  test("non-null-by ignores null operands") {
    val schema = new StructType().add("value", StringType.STRING).add("key", IntegerType.INTEGER)
    val aggregate = Aggregate.ungrouped(schema)
      .maxNonNullBy(column("value"), column("key"))
      .build()
    val input = Seq(
      row(schema, "ignored", null),
      row(schema, null, IntegerJ.valueOf(100)),
      row(schema, "winner", IntegerJ.valueOf(5)))

    checkRows(
      PlanBuilder.values(schema, input.asJava).aggregate(aggregate),
      Seq(row(aggregate.getSchema, "winner")))
  }

  test("materialize only the winning nested value") {
    val payloadType = new StructType().add("text", StringType.STRING)
    val schema = new StructType()
      .add("payload", payloadType)
      .add("key", IntegerType.INTEGER)
    val aggregate = Aggregate.ungrouped(schema)
      .maxNonNullBy(column("payload"), column("key"))
      .build()
    val oldValue = row(payloadType, "old")
    val newValue = row(payloadType, "new")
    val input = Seq(
      row(schema, oldValue, IntegerJ.valueOf(1)),
      row(schema, newValue, IntegerJ.valueOf(2)))

    checkRows(
      PlanBuilder.values(schema, input.asJava).aggregate(aggregate),
      Seq(row(aggregate.getSchema, newValue)))
  }

  test("reject unresolved columns and duplicate output names") {
    val schema = new StructType().add("value", IntegerType.INTEGER)
    val unresolved = intercept[IllegalArgumentException] {
      Aggregate.ungrouped(schema).min(column("missing")).build()
    }
    assert(unresolved.getMessage.contains("Aggregate value column"))

    val duplicate = intercept[IllegalArgumentException] {
      Aggregate.ungrouped(schema)
        .aggregateAs(Agg.min(column("value")), "result")
        .aggregateAs(Agg.max(column("value")), "RESULT")
        .build()
    }
    assert(duplicate.getMessage.contains("Duplicate aggregate output name"))
  }
}
