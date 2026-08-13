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

import java.lang.{Long => LongJ}
import java.util.Optional

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.expressions.{Column, Expression, Literal, Predicate, ScalarExpression}
import io.delta.kernel.internal.plans.Filter
import io.delta.kernel.types.{LongType, StringType, StructType}
import io.delta.kernel.utils.CloseableIterator

import PlanTestUtils._
import org.scalatest.funsuite.AnyFunSuite

class FilterExecutorSuite extends AnyFunSuite {
  private val schema = new StructType()
    .add("id", LongType.LONG)
    .add("label", StringType.STRING)

  private def batch(rows: (java.lang.Long, String)*): FilteredColumnarBatch =
    PlanTestUtils.batch(schema, rows.map { case (id, label) => row(schema, id, label) })

  private def column(name: String): Column = new Column(name)

  private def greaterThan(left: Expression, right: Expression): Predicate =
    new Predicate(">", left, right)

  private def filter(
      predicate: Predicate,
      input: CloseableIterator[FilteredColumnarBatch]): CloseableIterator[FilteredColumnarBatch] =
    FilterExecutor.execute(new Filter(predicate), schema, input)

  private def selectedIds(batch: FilteredColumnarBatch): Seq[Long] = {
    rows(batch).map(_.getLong(0))
  }

  test("Filter evaluates expressions with SQL null semantics without copying batch data") {
    val inputBatch = batch(
      LongJ.valueOf(1) -> "one",
      (null.asInstanceOf[java.lang.Long], "unknown"),
      LongJ.valueOf(3) -> "three",
      LongJ.valueOf(5) -> "five")
    val addOne = new ScalarExpression(
      "ADD",
      Seq[Expression](column("id"), Literal.ofLong(1)).asJava)
    val output = filter(
      greaterThan(addOne, Literal.ofLong(3)),
      new TrackingIterator(Seq(inputBatch)))

    try {
      assert(output.hasNext)
      val result = output.next()
      assert(result.getData eq inputBatch.getData)
      assert(result.getData.getSchema === schema)
      assert(selectedIds(result) === Seq(3L, 5L))
      assert(selection(result) === Seq(false, false, true, true))
      assert(!output.hasNext)
    } finally output.close()
  }

  test("Filter intersects an existing selection vector and treats null as unselected") {
    val data = batch(
      LongJ.valueOf(1) -> "one",
      LongJ.valueOf(3) -> "three",
      LongJ.valueOf(5) -> "five",
      LongJ.valueOf(7) -> "seven")
    val inputBatch = selectedBatch(data.getData, false, true, null, true)
    val output = filter(
      greaterThan(column("id"), Literal.ofLong(4)),
      new TrackingIterator(Seq(inputBatch)))

    try {
      val result = output.next()
      assert(selectedIds(result) === Seq(7L))
      assert(selection(result) === Seq(false, false, false, true))
    } finally output.close()
  }

  test("Filter preserves file metadata and invalidates a stale selected-row count") {
    val data = batch(LongJ.valueOf(1) -> "one", LongJ.valueOf(2) -> "two")
    val inputBatch = selectedBatch(
      data.getData,
      Seq(true, true).map(Boolean.box),
      "/table/part-000.parquet",
      2)
    val output = filter(
      greaterThan(column("id"), Literal.ofLong(1)),
      new TrackingIterator(Seq(inputBatch)))

    try {
      val result = output.next()
      assert(result.getData eq inputBatch.getData)
      assert(result.getFilePath === Optional.of("/table/part-000.parquet"))
      assert(result.getPreComputedNumSelectedRows.isEmpty)
      assert(selectedIds(result) === Seq(2L))
    } finally output.close()
  }

  test("Filter lazily returns one output for every input batch, including empty selections") {
    val first = batch(LongJ.valueOf(1) -> "one")
    val empty = batch()
    val last = batch(LongJ.valueOf(3) -> "three")
    val input = new TrackingIterator(Seq(first, empty, last))
    val output = filter(
      greaterThan(column("id"), Literal.ofLong(2)),
      input)

    assert(input.hasNextCalls === 0)
    assert(input.nextCalls === 0)
    try {
      assert(output.hasNext)
      assert(input.hasNextCalls === 1)
      assert(input.nextCalls === 0)

      val results = Seq(output.next(), output.next(), output.next())
      assert(results.map(_.getData.getSize) === Seq(1, 0, 1))
      assert(results.map(selectedIds) === Seq(Seq.empty, Seq.empty, Seq(3L)))
      assert(!output.hasNext)
    } finally output.close()

    assert(input.closeCalls === 1)
  }

  test("closing Filter closes its unconsumed input") {
    val input = new TrackingIterator(Seq(batch(LongJ.valueOf(1) -> "one")))
    val output = filter(new Predicate("IS_NOT_NULL", column("id")), input)

    output.close()

    assert(input.hasNextCalls === 0)
    assert(input.nextCalls === 0)
    assert(input.closeCalls === 1)
  }

  test("Filter preserves an empty input stream") {
    val input = new TrackingIterator[FilteredColumnarBatch](Seq.empty)
    val output = filter(new Predicate("IS_NOT_NULL", column("id")), input)

    try assert(!output.hasNext)
    finally output.close()

    assert(input.nextCalls === 0)
    assert(input.closeCalls === 1)
  }

  test("Filter rejects invalid setup before taking ownership of input") {
    val retained = new TrackingIterator(Seq(batch(LongJ.valueOf(1) -> "one")))

    assertThrows[NullPointerException](FilterExecutor.execute(null, schema, retained))
    assertThrows[NullPointerException](
      FilterExecutor.execute(
        new Filter(new Predicate("IS_NOT_NULL", column("id"))),
        null,
        retained))
    assertThrows[NullPointerException](
      FilterExecutor.execute(
        new Filter(new Predicate("IS_NOT_NULL", column("id"))),
        schema,
        null))
    val error = intercept[IllegalArgumentException] {
      filter(new Predicate("IS_NOT_NULL", column("missing")), retained)
    }

    assert(error.getMessage.contains("doesn't exist in input data schema"))
    assert(retained.hasNextCalls === 0)
    assert(retained.nextCalls === 0)
    assert(retained.closeCalls === 0)
  }

  test("callers can close Filter input after evaluation fails") {
    val wrongSchema = new StructType().add("other", LongType.LONG)
    val wrongBatch = PlanTestUtils.batch(
      wrongSchema,
      Seq(row(wrongSchema, LongJ.valueOf(1))))
    val input = new TrackingIterator(Seq(wrongBatch))
    val output = filter(new Predicate("IS_NOT_NULL", column("id")), input)

    val error = intercept[IllegalArgumentException] {
      try output.next()
      finally output.close()
    }

    assert(error.getMessage.contains("does not match expected schema"))
    assert(input.closeCalls === 1)
  }

  test("Filter rejects a null batch lazily and remains closeable") {
    val input = new TrackingIterator[FilteredColumnarBatch](Seq(null))
    val output = filter(new Predicate("IS_NOT_NULL", column("id")), input)

    val error = intercept[NullPointerException] {
      try output.next()
      finally output.close()
    }

    assert(error.getMessage === "input batch is null")
    assert(input.closeCalls === 1)
  }

  private def selection(batch: FilteredColumnarBatch): Seq[Boolean] = {
    val vector = batch.getSelectionVector.get()
    (0 until vector.getSize).map { rowId =>
      !vector.isNullAt(rowId) && vector.getBoolean(rowId)
    }
  }

}
