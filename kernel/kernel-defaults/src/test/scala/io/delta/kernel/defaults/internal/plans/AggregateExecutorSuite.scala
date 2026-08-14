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

import java.lang.{Float => FloatJ, Integer => IntegerJ}

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{FilteredColumnarBatch, Row, VariantValue}
import io.delta.kernel.defaults.internal.expressions.DefaultValueComparator
import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.plans.{Agg, Aggregate}
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types._

import PlanTestUtils._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll, Table}

class AggregateExecutorSuite extends AnyFunSuite {
  private def column(parts: String*): Column = new Column(parts.toArray)

  private def execute(
      aggregate: Aggregate,
      inputSchema: StructType,
      batches: Seq[FilteredColumnarBatch]): Seq[Row] = {
    val input = new TrackingIterator(batches)
    val output = AggregateExecutor.execute(aggregate, inputSchema, input)
    rows(output)
  }

  test("grouped aggregates consume selected rows across batches") {
    val schema = new StructType()
      .add("group", StringType.STRING)
      .add("score", IntegerType.INTEGER)
      .add("value", StringType.STRING)
      .add("order", IntegerType.INTEGER)
    val aggregate = Aggregate.groupBy(schema, Seq(column("group")).asJava)
      .aggregateAs(Agg.min(column("score")), "least")
      .aggregateAs(Agg.max(column("score")), "greatest")
      .aggregateAs(Agg.minNonNullBy(column("value"), column("order")), "earliest")
      .aggregateAs(Agg.maxNonNullBy(column("value"), column("order")), "latest")
      .build()
    val first = batch(
      schema,
      Seq(
        row(schema, "a", IntegerJ.valueOf(5), "five", IntegerJ.valueOf(5)),
        row(schema, "a", null, "three", IntegerJ.valueOf(3)),
        row(schema, "b", IntegerJ.valueOf(7), "seven", null),
        row(schema, "b", IntegerJ.valueOf(99), "ignored", IntegerJ.valueOf(99))),
      Some(Seq(true, true, true, false).map(Boolean.box)))
    val second = batch(
      schema,
      Seq(
        row(schema, "a", IntegerJ.valueOf(1), "one", IntegerJ.valueOf(1)),
        row(schema, "a", IntegerJ.valueOf(8), "eight", IntegerJ.valueOf(8)),
        row(schema, null, IntegerJ.valueOf(4), "four", IntegerJ.valueOf(4)),
        row(schema, "b", IntegerJ.valueOf(2), "two", IntegerJ.valueOf(2))))

    val rows = execute(aggregate, schema, Seq(first, second))

    assert(rows.map(_.getString(0)) === Seq("a", "b", null))
    assert(rows.map(_.getInt(1)) === Seq(1, 2, 4))
    assert(rows.map(_.getInt(2)) === Seq(8, 7, 4))
    assert(rows.map(_.getString(3)) === Seq("one", "two", "four"))
    assert(rows.map(_.getString(4)) === Seq("eight", "two", "four"))
  }

  test("global empty input emits one row with nullable aggregate results") {
    val schema = new StructType()
      .add("id", IntegerType.INTEGER)
      .add("value", StringType.STRING)
    val aggregate = Aggregate.ungrouped(schema)
      .min(column("id"))
      .maxNonNullBy(column("value"), column("id"))
      .build()

    val rows = execute(aggregate, schema, Seq.empty)
    assert(rows.size === 1)
    assert(rows.head.isNullAt(0))
    assert(rows.head.isNullAt(1))

    val emptyAggregate = Aggregate.ungrouped(schema).build()
    val emptyRows = execute(emptyAggregate, schema, Seq.empty)
    assert(emptyRows.size === 1)
    assert(emptyRows.head.getSchema.length() === 0)
  }

  test("non-null-by ignores null operands and keeps a deterministic first tie") {
    val schema = new StructType()
      .add("value", StringType.STRING)
      .add("key", IntegerType.INTEGER)
    val aggregate = Aggregate.ungrouped(schema)
      .aggregateAs(Agg.minNonNullBy(column("value"), column("key")), "minimum")
      .aggregateAs(Agg.maxNonNullBy(column("value"), column("key")), "maximum")
      .build()
    val input = batch(
      schema,
      Seq(
        row(schema, "ignored", null),
        row(schema, null, IntegerJ.valueOf(100)),
        row(schema, "first", IntegerJ.valueOf(5)),
        row(schema, "second", IntegerJ.valueOf(5))))

    val result = execute(aggregate, schema, Seq(input)).head
    assert(result.getString(0) === "first")
    assert(result.getString(1) === "first")
  }

  test("group keys use nested binary equality and normalize signed zero") {
    val partsType = new ArrayType(BinaryType.BINARY, false)
    val schema = new StructType()
      .add("parts", partsType)
      .add("number", FloatType.FLOAT)
      .add("value", IntegerType.INTEGER)
    val aggregate = Aggregate.groupBy(
      schema,
      Seq(column("parts"), column("number")).asJava)
      .max(column("value"))
      .build()
    val input = batch(
      schema,
      Seq(
        row(schema, binaryParts(1, 2), FloatJ.valueOf(0.0f), IntegerJ.valueOf(1)),
        row(schema, binaryParts(1, 2), FloatJ.valueOf(-0.0f), IntegerJ.valueOf(2)),
        row(schema, binaryParts(3), FloatJ.valueOf(Float.NaN), IntegerJ.valueOf(3)),
        row(schema, binaryParts(3), FloatJ.valueOf(Float.NaN), IntegerJ.valueOf(4))))

    val results = execute(aggregate, schema, Seq(input))
    assert(results.size === 2)
    assert(results.map(_.getInt(2)) === Seq(2, 4))
  }

  test("Variant group keys use encoded value and metadata equality") {
    val schema = new StructType()
      .add("key", VariantType.VARIANT)
      .add("value", IntegerType.INTEGER)
    val aggregate = Aggregate.groupBy(schema, Seq(column("key")).asJava)
      .max(column("value"))
      .build()
    val first = new VariantValue(Array[Byte](1), Array[Byte](10))
    val equal = new VariantValue(Array[Byte](1), Array[Byte](10))
    val other = new VariantValue(Array[Byte](2), Array[Byte](10))
    val input = batch(
      schema,
      Seq(
        row(schema, first, IntegerJ.valueOf(1)),
        row(schema, equal, IntegerJ.valueOf(2)),
        row(schema, other, IntegerJ.valueOf(3))))

    val results = execute(aggregate, schema, Seq(input))
    assert(results.map(_.getVariant(0)) === Seq(first, other))
    assert(results.map(_.getInt(1)) === Seq(2, 3))
    val ordering = Aggregate.ungrouped(schema).min(column("key")).build()
    assertThrows[UnsupportedOperationException](execute(ordering, schema, Seq.empty))
  }

  private def binaryParts(values: Byte*): io.delta.kernel.data.ArrayValue =
    VectorUtils.buildArrayValue(
      values.map(value => Array(value)).asJava,
      BinaryType.BINARY)

  test("non-null-by returns nested Kernel rows with stripped field metadata") {
    val metadata = FieldMetadata.builder().putString("source", "input").build()
    val payloadType = new StructType().add("text", StringType.STRING, true, metadata)
    val schema = new StructType()
      .add("payload", payloadType, true, metadata)
      .add("key", IntegerType.INTEGER, false)
    val aggregate = Aggregate.ungrouped(schema)
      .aggregateAs(Agg.maxNonNullBy(column("payload"), column("key")), "winner")
      .build()
    val input = batch(
      schema,
      Seq(
        row(schema, row(payloadType, "old"), IntegerJ.valueOf(1)),
        row(schema, row(payloadType, "new"), IntegerJ.valueOf(2))))

    val result = execute(aggregate, schema, Seq(input)).head
    assert(result.getStruct(0).getString(0) === "new")
    val outputType = result.getSchema.at(0).getDataType.asInstanceOf[StructType]
    assert(outputType.at(0).getMetadata === FieldMetadata.empty())
  }

  test("non-null-by detaches retained complex values from input vectors") {
    val binariesType = new ArrayType(BinaryType.BINARY, false)
    val lookupType = new MapType(StringType.STRING, BinaryType.BINARY, false)
    val payloadType = new StructType()
      .add("bytes", BinaryType.BINARY)
      .add("binaries", binariesType)
      .add("lookup", lookupType)
    val schema = new StructType()
      .add("payload", payloadType)
      .add("key", IntegerType.INTEGER)
    val bytes = Array[Byte](1)
    val arrayBytes = Array[Byte](2)
    val mapBytes = Array[Byte](3)
    val payload = row(
      payloadType,
      bytes,
      VectorUtils.buildArrayValue(Seq(arrayBytes).asJava, BinaryType.BINARY),
      VectorUtils.buildMapValue(
        Seq("key").asJava,
        Seq(mapBytes).asJava,
        lookupType))
    val aggregate = Aggregate.ungrouped(schema)
      .aggregateAs(Agg.maxNonNullBy(column("payload"), column("key")), "payload")
      .build()

    val result = execute(
      aggregate,
      schema,
      Seq(batch(schema, Seq(row(schema, payload, IntegerJ.valueOf(1)))))).head
    bytes(0) = 11
    arrayBytes(0) = 12
    mapBytes(0) = 13

    val retained = result.getStruct(0)
    assert(retained.getBinary(0).sameElements(Array[Byte](1)))
    assert(retained.getArray(1).getElements.getBinary(0).sameElements(Array[Byte](2)))
    assert(retained.getMap(2).getValues.getBinary(0).sameElements(Array[Byte](3)))
  }

  test("unsupported grouping and ordering types fail before consuming input") {
    val arrayType = new ArrayType(IntegerType.INTEGER, true)
    val mapType = new MapType(StringType.STRING, IntegerType.INTEGER, true)
    val schema = new StructType()
      .add("items", arrayType)
      .add("lookup", mapType)
    val cases = Table(
      "aggregate",
      Aggregate.groupBy(schema, Seq(column("lookup")).asJava).build(),
      Aggregate.ungrouped(schema).min(column("items")).build())

    forAll(cases) { aggregate =>
      val input = new TrackingIterator[FilteredColumnarBatch](Seq.empty)
      assertThrows[UnsupportedOperationException] {
        AggregateExecutor.execute(aggregate, schema, input)
      }
      assert(input.hasNextCalls === 0)
      assert(input.closeCalls === 0)
    }
  }

  test("Aggregate eagerly closes its input and rejects mismatched batches") {
    val schema = new StructType().add("id", IntegerType.INTEGER)
    val aggregate = Aggregate.ungrouped(schema).max(column("id")).build()
    val input = new TrackingIterator(Seq(batch(schema, Seq(row(schema, IntegerJ.valueOf(1))))))

    val output = AggregateExecutor.execute(aggregate, schema, input)
    assert(input.closeCalls === 1)
    output.close()

    val otherSchema = new StructType().add("other", IntegerType.INTEGER)
    val mismatch = new TrackingIterator(
      Seq(batch(otherSchema, Seq(row(otherSchema, IntegerJ.valueOf(1))))))
    val error = intercept[IllegalArgumentException] {
      AggregateExecutor.execute(aggregate, schema, mismatch)
    }
    assert(error.getMessage.contains("does not match expected schema"))
    assert(mismatch.closeCalls === 1)
  }

  test("shared comparator orders all supported primitive families") {
    val cases = Table(
      ("type", "lower", "higher"),
      (BooleanType.BOOLEAN: DataType, false, true),
      (IntegerType.INTEGER: DataType, IntegerJ.valueOf(1), IntegerJ.valueOf(2)),
      (
        IntervalYearMonthType.INTERVAL_YEAR_MONTH: DataType,
        IntegerJ.valueOf(-1),
        IntegerJ.valueOf(1)),
      (LongType.LONG: DataType, java.lang.Long.valueOf(1), java.lang.Long.valueOf(2)),
      (
        IntervalDayTimeType.INTERVAL_DAY_TIME: DataType,
        java.lang.Long.valueOf(-1),
        java.lang.Long.valueOf(1)),
      (FloatType.FLOAT: DataType, FloatJ.valueOf(1), FloatJ.valueOf(2)),
      (DoubleType.DOUBLE: DataType, java.lang.Double.valueOf(1), java.lang.Double.valueOf(2)),
      (StringType.STRING: DataType, "a", "b"),
      (BinaryType.BINARY: DataType, Array[Byte](0), Array[Byte](1)))

    forAll(cases) { (dataType, lower, higher) =>
      assert(DefaultValueComparator.compare(dataType, lower, higher) < 0)
      assert(DefaultValueComparator.compare(dataType, higher, lower) > 0)
      assert(DefaultValueComparator.compare(dataType, lower, lower) === 0)
    }
  }

  test("Aggregate groups and orders both interval families") {
    val yearMonth = IntervalYearMonthType.INTERVAL_YEAR_MONTH
    val dayTime = IntervalDayTimeType.INTERVAL_DAY_TIME
    val schema = new StructType()
      .add("group", yearMonth)
      .add("value", dayTime)
    val aggregate = Aggregate.groupBy(schema, Seq(column("group")).asJava)
      .aggregateAs(Agg.min(column("value")), "minimum")
      .aggregateAs(Agg.max(column("value")), "maximum")
      .build()
    val input = batch(
      schema,
      Seq(
        row(schema, Int.box(-13), Long.box(5L)),
        row(schema, Int.box(-13), Long.box(-5L)),
        row(schema, Int.box(30), Long.box(Long.MaxValue))))

    val result = execute(aggregate, schema, Seq(input))
    assert(result.map(_.getIntervalYearMonth(0)) === Seq(-13, 30))
    assert(result.map(_.getIntervalDayTime(1)) === Seq(-5L, Long.MaxValue))
    assert(result.map(_.getIntervalDayTime(2)) === Seq(5L, Long.MaxValue))
  }

}
