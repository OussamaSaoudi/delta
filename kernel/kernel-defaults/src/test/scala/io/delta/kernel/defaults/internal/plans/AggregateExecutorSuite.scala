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
import java.util.Optional

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ColumnVector, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector
import io.delta.kernel.defaults.internal.expressions.DefaultValueComparator
import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.plans.{Agg, Aggregate}
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll, Table}

class AggregateExecutorSuite extends AnyFunSuite {
  private def column(parts: String*): Column = new Column(parts.toArray)

  private def row(schema: StructType, values: AnyRef*): Row =
    GenericRow.fromValues(schema, values.asJava)

  private def batch(
      schema: StructType,
      rows: Seq[Row],
      selected: Option[Seq[java.lang.Boolean]] = None): FilteredColumnarBatch = {
    val selection = selected.map { values =>
      val nulls = values.map(_ == null).toArray
      val booleans = values.map(value => value != null && value.booleanValue()).toArray
      new DefaultBooleanVector(values.size, Optional.of(nulls), booleans): ColumnVector
    }
    new FilteredColumnarBatch(
      new DefaultRowBasedColumnarBatch(schema, rows.asJava),
      Optional.ofNullable(selection.orNull))
  }

  private def execute(
      aggregate: Aggregate,
      inputSchema: StructType,
      batches: Seq[FilteredColumnarBatch]): Seq[Row] = {
    val input = Utils.toCloseableIterator(batches.iterator.asJava)
    val output = AggregateExecutor.execute(aggregate, inputSchema, input)
    try {
      assert(output.hasNext)
      val rows = output.next().getRows
      try rows.asScala.toSeq
      finally rows.close()
    } finally output.close()
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

  test("group keys use deep binary equality and normalize signed zero") {
    val schema = new StructType()
      .add("bytes", BinaryType.BINARY)
      .add("number", FloatType.FLOAT)
      .add("value", IntegerType.INTEGER)
    val aggregate = Aggregate.groupBy(
      schema,
      Seq(column("bytes"), column("number")).asJava)
      .max(column("value"))
      .build()
    val input = batch(
      schema,
      Seq(
        row(schema, Array[Byte](1, 2), FloatJ.valueOf(0.0f), IntegerJ.valueOf(1)),
        row(schema, Array[Byte](1, 2), FloatJ.valueOf(-0.0f), IntegerJ.valueOf(2)),
        row(schema, Array[Byte](3), FloatJ.valueOf(Float.NaN), IntegerJ.valueOf(3)),
        row(schema, Array[Byte](3), FloatJ.valueOf(Float.NaN), IntegerJ.valueOf(4))))

    val results = execute(aggregate, schema, Seq(input))
    assert(results.size === 2)
    assert(results.map(_.getInt(2)) === Seq(2, 4))
  }

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
      val input = new TrackingIterator(Seq.empty)
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
      (LongType.LONG: DataType, java.lang.Long.valueOf(1), java.lang.Long.valueOf(2)),
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

  private class TrackingIterator(batches: Seq[FilteredColumnarBatch])
      extends CloseableIterator[FilteredColumnarBatch] {
    private var index = 0
    var hasNextCalls = 0
    var closeCalls = 0

    override def hasNext: Boolean = {
      hasNextCalls += 1
      index < batches.size
    }

    override def next(): FilteredColumnarBatch = {
      val result = batches(index)
      index += 1
      result
    }

    override def close(): Unit = closeCalls += 1
  }
}
