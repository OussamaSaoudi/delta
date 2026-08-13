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
import java.util

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.internal.plans.Values
import io.delta.kernel.types.{LongType, StringType, StructType}
import io.delta.kernel.utils.CloseableIterator

import PlanTestUtils._
import org.scalatest.funsuite.AnyFunSuite

class ValuesUnionExecutorSuite extends AnyFunSuite {
  private type BatchIterator = CloseableIterator[FilteredColumnarBatch]

  private val schema = new StructType()
    .add("id", LongType.LONG, false)
    .add("label", StringType.STRING)

  private def values(rows: (Long, String)*): Values =
    new Values(
      schema,
      rows.map { case (id, label) => row(schema, LongJ.valueOf(id), label) }.asJava)

  private def batch(ids: Long*): FilteredColumnarBatch = {
    val iterator = ValuesExecutor.execute(values(ids.map(id => id -> s"value-$id"): _*))
    try iterator.next()
    finally iterator.close()
  }

  private def batchIds(batch: FilteredColumnarBatch): Seq[Long] = {
    rows(batch).map(_.getLong(0))
  }

  private def unionAll(inputs: BatchIterator*): BatchIterator =
    UnionAllExecutor.execute(inputs.asJava)

  test("Values emits one correctly typed unfiltered batch") {
    val iterator = ValuesExecutor.execute(values(1L -> "one", 2L -> null))
    try {
      assert(iterator.hasNext)
      val result = iterator.next()
      assert(result.getData.getSchema === schema)
      assert(result.getData.getSize === 2)
      assert(result.getSelectionVector.isEmpty)
      assert(result.getData.getColumnVector(0).getLong(0) === 1L)
      assert(result.getData.getColumnVector(1).getString(0) === "one")
      assert(result.getData.getColumnVector(1).isNullAt(1))
      assert(!iterator.hasNext)
    } finally iterator.close()
  }

  test("empty Values emits one empty batch carrying its schema") {
    val iterator = ValuesExecutor.execute(values())
    try {
      assert(iterator.hasNext)
      val result = iterator.next()
      assert(result.getData.getSchema === schema)
      assert(result.getData.getSize === 0)
      assert(result.getSelectionVector.isEmpty)
      assert(!iterator.hasNext)
    } finally iterator.close()
  }

  test("UnionAll lazily preserves input and batch order without copies") {
    val first = ValuesExecutor.execute(values(1L -> "one", 2L -> "two"))
    val empty = ValuesExecutor.execute(values())
    val last = ValuesExecutor.execute(values(3L -> "three"))
    val result = UnionAllExecutor.execute(Seq(first, empty, last).asJava).toInMemoryList()

    assert(result.asScala.map(_.getData.getSchema) === Seq(schema, schema, schema))
    assert(result.asScala.map(_.getData.getSize) === Seq(2, 0, 1))
    assert(result.asScala.flatMap(batchIds) === Seq(1L, 2L, 3L))
  }

  test("UnionAll does not touch later inputs before they are needed") {
    val leftBatch = batch(1L)
    val rightBatch = batch(2L)
    val left = new TrackingIterator(Seq(leftBatch))
    val right = new TrackingIterator(Seq(rightBatch))
    val union = unionAll(left, right)

    try {
      assert(left.hasNextCalls === 0)
      assert(right.hasNextCalls === 0)
      assert(union.hasNext)
      assert(left.hasNextCalls === 1)
      assert(right.hasNextCalls === 0)
      assert(union.next() eq leftBatch)
      assert(union.hasNext)
      assert(right.hasNextCalls === 1)
      assert(union.next() eq rightBatch)
    } finally union.close()

    assert(left.closeCalls === 1)
    assert(right.closeCalls === 1)
  }

  test("closing UnionAll closes every child, including unopened inputs") {
    val children = Seq(
      new TrackingIterator(Seq(batch(1L))),
      new TrackingIterator(Seq(batch(2L))),
      new TrackingIterator(Seq(batch(3L))))
    val union = unionAll(children: _*)

    union.close()

    assert(children.map(_.closeCalls) === Seq(1, 1, 1))
    assert(children.map(_.hasNextCalls) === Seq(0, 0, 0))
  }

  test("callers can close all UnionAll children after an iteration failure") {
    val failure = new RuntimeException("boom")
    val first = new TrackingIterator[FilteredColumnarBatch](Seq.empty)
    val failing = new TrackingIterator[FilteredColumnarBatch](
      Seq.empty,
      hasNextFailure = Some(failure))
    val unopened = new TrackingIterator(Seq(batch(3L)))
    val union = unionAll(first, failing, unopened)

    val thrown = intercept[RuntimeException] {
      try union.hasNext
      finally union.close()
    }

    assert(thrown eq failure)
    assert(Seq(first, failing, unopened).map(_.closeCalls) === Seq(1, 1, 1))
    assert(unopened.hasNextCalls === 0)
  }

  test("UnionAll attempts every child close when closes fail") {
    val children = Seq(
      new TrackingIterator[FilteredColumnarBatch](
        Seq.empty,
        closeFailure = Some(new RuntimeException("close failed"))),
      new TrackingIterator[FilteredColumnarBatch](
        Seq.empty,
        closeFailure = Some(new RuntimeException("close failed"))),
      new TrackingIterator[FilteredColumnarBatch](
        Seq.empty,
        closeFailure = Some(new RuntimeException("close failed"))))
    val union = unionAll(children: _*)

    assertThrows[RuntimeException](union.close())
    assert(children.map(_.closeCalls) === Seq(1, 1, 1))
  }

  test("UnionAll validates inputs before taking ownership") {
    assertThrows[NullPointerException](UnionAllExecutor.execute(null))
    assertThrows[IllegalArgumentException](
      UnionAllExecutor.execute(util.Collections.emptyList()))

    val retained = new TrackingIterator(Seq(batch(1L)))
    val inputs = new util.ArrayList[CloseableIterator[FilteredColumnarBatch]]()
    inputs.add(retained)
    inputs.add(null)
    val error = intercept[NullPointerException] {
      UnionAllExecutor.execute(inputs)
    }

    assert(error.getMessage.contains("input iterator 1 is null"))
    assert(retained.closeCalls === 0)
  }

}
