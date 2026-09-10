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

import java.lang.{Double => DoubleJ, Integer => IntegerJ, Long => LongJ}

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.expressions.{Column, Expression}
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.plans.{Agg, Aggregate, PlanNode, UnionAll, Values}
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class AggregatePlanSuite extends AnyFunSuite with PlanExecutionSuiteBase {
  private def column(name: String): Column = new Column(name)

  private def row(schema: StructType, values: AnyRef*): Row =
    GenericRow.fromValues(schema, values.asJava)

  private def values(schema: StructType, rows: Row*): Values =
    new Values(schema, rows.asJava)

  private def aggregate(
      input: PlanNode,
      groups: Seq[Expression],
      aggregates: Seq[Agg],
      outputSchema: StructType): Aggregate =
    new Aggregate(input, groups.asJava, aggregates.asJava, outputSchema)

  test("execute every ungrouped aggregate across input batches") {
    val schema = new StructType()
      .add("score", IntegerType.INTEGER)
      .add("value", StringType.STRING)
      .add("order", IntegerType.INTEGER)
      .add("amount", LongType.LONG)
    val first = values(
      schema,
      row(schema, IntegerJ.valueOf(5), "five", IntegerJ.valueOf(5), LongJ.valueOf(10)),
      row(schema, null, "three", IntegerJ.valueOf(3), null),
      row(schema, IntegerJ.valueOf(7), null, IntegerJ.valueOf(7), LongJ.valueOf(20)))
    val second = values(
      schema,
      row(schema, IntegerJ.valueOf(1), "one", IntegerJ.valueOf(1), LongJ.valueOf(30)),
      row(schema, IntegerJ.valueOf(8), "eight", IntegerJ.valueOf(8), LongJ.valueOf(40)))
    val input = new UnionAll(Seq[PlanNode](first, second).asJava)
    val outputSchema = new StructType()
      .add("least", IntegerType.INTEGER, true)
      .add("greatest", IntegerType.INTEGER, true)
      .add("earliest", StringType.STRING, true)
      .add("latest", StringType.STRING, true)
      .add("total", LongType.LONG, true)
      .add("score_count", LongType.LONG, false)
      .add("row_count", LongType.LONG, false)
    val plan = aggregate(
      input,
      Seq.empty,
      Seq(
        Agg.min(column("score"), IntegerType.INTEGER),
        Agg.max(column("score"), IntegerType.INTEGER),
        Agg.minNonNullBy(
          column("value"),
          StringType.STRING,
          column("order"),
          IntegerType.INTEGER,
          column("order"),
          IntegerType.INTEGER),
        Agg.maxNonNullBy(
          column("value"),
          StringType.STRING,
          column("order"),
          IntegerType.INTEGER,
          column("order"),
          IntegerType.INTEGER),
        Agg.sum(column("amount")),
        Agg.count(column("score"), IntegerType.INTEGER),
        Agg.countStar()),
      outputSchema)

    assert(outputSchema.fieldNames.asScala === Seq(
      "least",
      "greatest",
      "earliest",
      "latest",
      "total",
      "score_count",
      "row_count"))
    checkRows(
      plan,
      Seq(row(
        outputSchema,
        IntegerJ.valueOf(1),
        IntegerJ.valueOf(8),
        "one",
        "eight",
        LongJ.valueOf(100),
        LongJ.valueOf(4),
        LongJ.valueOf(5))))
  }

  test("empty input emits null values and zero counts") {
    val schema = new StructType()
      .add("id", IntegerType.INTEGER)
      .add("amount", LongType.LONG)
    val outputSchema = new StructType()
      .add("minimum", IntegerType.INTEGER, true)
      .add("maximum", IntegerType.INTEGER, true)
      .add("total", LongType.LONG, true)
      .add("present", LongType.LONG, false)
      .add("rows", LongType.LONG, false)
    val plan = aggregate(
      values(schema),
      Seq.empty,
      Seq(
        Agg.min(column("id"), IntegerType.INTEGER),
        Agg.max(column("id"), IntegerType.INTEGER),
        Agg.sum(column("amount")),
        Agg.count(column("id"), IntegerType.INTEGER),
        Agg.countStar()),
      outputSchema)

    checkRows(
      plan,
      Seq(row(
        outputSchema,
        null,
        null,
        null,
        LongJ.valueOf(0),
        LongJ.valueOf(0))))
  }

  test("group rows across batches with null-safe keys") {
    val schema = new StructType()
      .add("group", StringType.STRING)
      .add("score", IntegerType.INTEGER)
      .add("value", StringType.STRING)
      .add("order", IntegerType.INTEGER)
    val first = values(
      schema,
      row(schema, "a", IntegerJ.valueOf(5), "five", IntegerJ.valueOf(5)),
      row(schema, null, IntegerJ.valueOf(2), "two", IntegerJ.valueOf(2)))
    val second = values(
      schema,
      row(schema, "a", IntegerJ.valueOf(8), "eight", IntegerJ.valueOf(8)),
      row(schema, null, IntegerJ.valueOf(1), "one", IntegerJ.valueOf(1)))
    val outputSchema = new StructType()
      .add("group", StringType.STRING, true)
      .add("least", IntegerType.INTEGER, true)
      .add("greatest", IntegerType.INTEGER, true)
      .add("earliest", StringType.STRING, true)
      .add("latest", StringType.STRING, true)
    val plan = aggregate(
      new UnionAll(Seq[PlanNode](first, second).asJava),
      Seq(column("group")),
      Seq(
        Agg.min(column("score"), IntegerType.INTEGER),
        Agg.max(column("score"), IntegerType.INTEGER),
        Agg.minNonNullBy(
          column("value"),
          StringType.STRING,
          column("order"),
          IntegerType.INTEGER,
          column("order"),
          IntegerType.INTEGER),
        Agg.maxNonNullBy(
          column("value"),
          StringType.STRING,
          column("order"),
          IntegerType.INTEGER,
          column("order"),
          IntegerType.INTEGER)),
      outputSchema)

    checkRowsUnordered(
      plan,
      Seq(
        row(
          outputSchema,
          "a",
          IntegerJ.valueOf(5),
          IntegerJ.valueOf(8),
          "five",
          "eight"),
        row(
          outputSchema,
          null,
          IntegerJ.valueOf(1),
          IntegerJ.valueOf(2),
          "one",
          "two")))
  }

  test("grouping normalizes signed zero and compares binary by contents") {
    val numberSchema = new StructType()
      .add("group", DoubleType.DOUBLE)
      .add("value", IntegerType.INTEGER)
    val numberOutput = new StructType()
      .add("group", DoubleType.DOUBLE, true)
      .add("maximum", IntegerType.INTEGER, true)
    val numberPlan = aggregate(
      values(
        numberSchema,
        row(numberSchema, DoubleJ.valueOf(-0.0d), IntegerJ.valueOf(1)),
        row(numberSchema, DoubleJ.valueOf(0.0d), IntegerJ.valueOf(2))),
      Seq(column("group")),
      Seq(Agg.max(column("value"), IntegerType.INTEGER)),
      numberOutput)

    checkRows(
      numberPlan,
      Seq(row(numberOutput, DoubleJ.valueOf(-0.0d), IntegerJ.valueOf(2))))

    val binarySchema = new StructType()
      .add("group", BinaryType.BINARY)
      .add("value", IntegerType.INTEGER)
    val binaryOutput = new StructType()
      .add("group", BinaryType.BINARY, true)
      .add("maximum", IntegerType.INTEGER, true)
    val binaryPlan = aggregate(
      values(
        binarySchema,
        row(binarySchema, Array[Byte](1, 2), IntegerJ.valueOf(1)),
        row(binarySchema, Array[Byte](1, 2), IntegerJ.valueOf(2))),
      Seq(column("group")),
      Seq(Agg.max(column("value"), IntegerType.INTEGER)),
      binaryOutput)

    checkRows(
      binaryPlan,
      Seq(row(binaryOutput, Array[Byte](1, 2), IntegerJ.valueOf(2))))
  }

  test("three-operand non-null-by qualifies on sentinel and key, not value") {
    val schema = new StructType()
      .add("value", StringType.STRING)
      .add("sentinel", IntegerType.INTEGER)
      .add("key", IntegerType.INTEGER)
    val outputSchema = new StructType()
      .add("earliest", StringType.STRING, true)
      .add("latest", StringType.STRING, true)
    val plan = aggregate(
      values(
        schema,
        row(schema, "bad-sentinel", null, IntegerJ.valueOf(1000)),
        row(schema, "bad-key", IntegerJ.valueOf(1), null),
        row(schema, null, IntegerJ.valueOf(1), IntegerJ.valueOf(100)),
        row(schema, "winner", IntegerJ.valueOf(1), IntegerJ.valueOf(5))),
      Seq.empty,
      Seq(
        Agg.minNonNullBy(
          column("value"),
          StringType.STRING,
          column("sentinel"),
          IntegerType.INTEGER,
          column("key"),
          IntegerType.INTEGER),
        Agg.maxNonNullBy(
          column("value"),
          StringType.STRING,
          column("sentinel"),
          IntegerType.INTEGER,
          column("key"),
          IntegerType.INTEGER)),
      outputSchema)

    checkRows(plan, Seq(row(outputSchema, "winner", null)))
  }

  test("non-null-by materializes only the winning nested value") {
    val payloadType = new StructType().add("text", StringType.STRING)
    val schema = new StructType()
      .add("payload", payloadType)
      .add("key", IntegerType.INTEGER)
    val outputSchema = new StructType().add("payload", payloadType, true)
    val oldValue = row(payloadType, "old")
    val newValue = row(payloadType, "new")
    val plan = aggregate(
      values(
        schema,
        row(schema, oldValue, IntegerJ.valueOf(1)),
        row(schema, newValue, IntegerJ.valueOf(2))),
      Seq.empty,
      Seq(Agg.maxNonNullBy(
        column("payload"),
        payloadType,
        column("key"),
        IntegerType.INTEGER,
        column("key"),
        IntegerType.INTEGER)),
      outputSchema)

    checkRows(plan, Seq(row(outputSchema, newValue)))
  }

  test("reject unresolved columns and invalid output types") {
    val schema = new StructType().add("value", IntegerType.INTEGER)
    val input = values(schema)
    val unresolved = intercept[IllegalArgumentException] {
      aggregate(
        input,
        Seq.empty,
        Seq(Agg.min(column("missing"), IntegerType.INTEGER)),
        new StructType().add("result", IntegerType.INTEGER, true))
    }
    assert(unresolved.getMessage.contains("Aggregate value"))

    val invalidOutput = intercept[IllegalArgumentException] {
      aggregate(
        input,
        Seq.empty,
        Seq(Agg.max(column("value"), IntegerType.INTEGER)),
        new StructType().add("result", StringType.STRING, true))
    }
    assert(invalidOutput.getMessage.contains("must have type"))
  }
}
