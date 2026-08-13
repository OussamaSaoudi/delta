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

import io.delta.kernel.data.{ColumnVector, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector
import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.plans.{SemiJoin, Values}
import io.delta.kernel.types.{BinaryType, LongType, StringType, StructType}
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class SemiJoinExecutorSuite extends AnyFunSuite {
  private val probeSchema = new StructType()
    .add("id", LongType.LONG)
    .add("tag", StringType.STRING)
  private val buildSchema = new StructType()
    .add("build_id", LongType.LONG)
    .add("build_tag", StringType.STRING)

  private def probeRow(id: java.lang.Long, tag: String): Row =
    GenericRow.fromValues(probeSchema, Seq(id, tag).asJava)

  private def buildRow(id: java.lang.Long, tag: String): Row =
    GenericRow.fromValues(buildSchema, Seq(id, tag).asJava)

  private def batch(schema: StructType, rows: Seq[Row]): FilteredColumnarBatch = {
    val iterator = ValuesExecutor.execute(new Values(schema, rows.asJava))
    try iterator.next()
    finally iterator.close()
  }

  private def probeBatch(rows: (java.lang.Long, String)*): FilteredColumnarBatch =
    batch(probeSchema, rows.map { case (id, tag) => probeRow(id, tag) })

  private def buildBatch(rows: (java.lang.Long, String)*): FilteredColumnarBatch =
    batch(buildSchema, rows.map { case (id, tag) => buildRow(id, tag) })

  private def join(
      inverted: Boolean,
      probe: CloseableIterator[FilteredColumnarBatch],
      build: CloseableIterator[FilteredColumnarBatch],
      probeKeys: Seq[String] = Seq("id", "tag"),
      buildKeys: Seq[String] = Seq("build_id", "build_tag")) = {
    val operator = new SemiJoin(
      inverted,
      probeKeys.map(new Column(_)).asJava,
      buildKeys.map(new Column(_)).asJava)
    SemiJoinExecutor.execute(operator, probeSchema, buildSchema, probe, build)
  }

  for (
    (inverted, expected) <- Seq(
      false -> Seq((1L, "one"), (1L, null)),
      true -> Seq((2L, "two"), (3L, "three")))
  ) {
    test(s"${if (inverted) "anti" else "semi"} join uses null-safe compound-key set semantics") {
      val probe = new TrackingIterator(Seq(probeBatch(
        LongJ.valueOf(1) -> "one",
        LongJ.valueOf(1) -> null,
        LongJ.valueOf(2) -> "two",
        LongJ.valueOf(3) -> "three")))
      val build = new TrackingIterator(Seq(buildBatch(
        LongJ.valueOf(1) -> "one",
        LongJ.valueOf(1) -> null,
        LongJ.valueOf(1) -> "one")))
      val output = join(inverted, probe, build)

      try assert(selected(output.next()) === expected)
      finally output.close()

      assert(build.nextCalls === 1)
      assert(build.closeCalls === 1)
      assert(probe.closeCalls === 1)
    }
  }

  test("SemiJoin honors build and probe selections across batches without copying probe data") {
    val rawBuild = buildBatch(
      LongJ.valueOf(1) -> "one",
      LongJ.valueOf(2) -> "two",
      LongJ.valueOf(3) -> "three")
    val selectedBuild = new FilteredColumnarBatch(
      rawBuild.getData,
      Optional.of(booleanVector(true, false, true)))
    val first = probeBatch(LongJ.valueOf(1) -> "one", LongJ.valueOf(2) -> "two")
    val rawSecond = probeBatch(LongJ.valueOf(3) -> "three", LongJ.valueOf(1) -> "one")
    val second = new FilteredColumnarBatch(
      rawSecond.getData,
      Optional.of(booleanVector(false, true)),
      "/table/probe.parquet",
      1)
    val output = join(
      false,
      new TrackingIterator(Seq(first, second)),
      new TrackingIterator(Seq(selectedBuild, buildBatch(LongJ.valueOf(1) -> "one"))))

    try {
      val result1 = output.next()
      val result2 = output.next()
      assert(result1.getData eq first.getData)
      assert(result2.getData eq second.getData)
      assert(selected(result1) === Seq(1L -> "one"))
      assert(selected(result2) === Seq(1L -> "one"))
      assert(result2.getFilePath === Optional.of("/table/probe.parquet"))
      assert(result2.getPreComputedNumSelectedRows.isEmpty)
      assert(!output.hasNext)
    } finally output.close()
  }

  for (
    (inverted, expected) <- Seq(
      false -> Seq.empty[(Long, String)],
      true -> Seq(1L -> "one"))
  ) {
    test(s"${if (inverted) "AntiJoin" else "SemiJoin"} handles an empty build") {
      val output = join(
        inverted,
        new TrackingIterator(Seq(probeBatch(LongJ.valueOf(1) -> "one"))),
        new TrackingIterator(Seq.empty))
      try assert(selected(output.next()) === expected)
      finally output.close()
    }
  }

  test("SemiJoin compares binary keys by content") {
    val probeBinarySchema = new StructType().add("key", BinaryType.BINARY)
    val buildBinarySchema = new StructType().add("other", BinaryType.BINARY)
    def binaryBatch(schema: StructType, values: Seq[Array[Byte]]) =
      batch(schema, values.map(value => GenericRow.fromValues(schema, Seq(value).asJava)))
    val operator = new SemiJoin(
      false,
      Seq(new Column("key")).asJava,
      Seq(new Column("other")).asJava)
    val probeBatch = binaryBatch(probeBinarySchema, Seq(Array[Byte](1, 2), Array[Byte](3)))
    val output = SemiJoinExecutor.execute(
      operator,
      probeBinarySchema,
      buildBinarySchema,
      new TrackingIterator(Seq(probeBatch)),
      new TrackingIterator(Seq(binaryBatch(buildBinarySchema, Seq(Array[Byte](1, 2))))))

    try {
      val rows = output.next().getRows
      try assert(rows.asScala.map(_.getBinary(0).toSeq).toSeq === Seq(Seq[Byte](1, 2)))
      finally rows.close()
    } finally output.close()
  }

  test("SemiJoin eagerly consumes and closes build but leaves probe lazy") {
    val probe = new TrackingIterator(Seq(probeBatch(LongJ.valueOf(1) -> "one")))
    val build = new TrackingIterator(Seq(
      buildBatch(LongJ.valueOf(1) -> "one"),
      buildBatch(LongJ.valueOf(2) -> "two")))

    val output = join(false, probe, build)

    assert(build.nextCalls === 2)
    assert(build.closeCalls === 1)
    assert(probe.hasNextCalls === 0)
    assert(probe.nextCalls === 0)
    output.close()
    assert(probe.closeCalls === 1)
  }

  test("SemiJoin closes both inputs when build iteration fails and preserves the failure") {
    val failure = new IllegalStateException("build failed")
    val probe = new TrackingIterator(Seq(probeBatch(LongJ.valueOf(1) -> "one")))
    val build = new TrackingIterator(Seq.empty, Some(failure))

    val thrown = intercept[IllegalStateException] {
      join(false, probe, build)
    }

    assert(thrown eq failure)
    assert(build.closeCalls === 1)
    assert(probe.closeCalls === 1)
  }

  test("SemiJoin closes probe when closing the materialized build fails") {
    val closeFailure = new IllegalStateException("build close failed")
    val probe = new TrackingIterator(Seq(probeBatch(LongJ.valueOf(1) -> "one")))
    val build = new TrackingIterator(
      Seq(buildBatch(LongJ.valueOf(1) -> "one")),
      closeFailure = Some(closeFailure))

    val thrown = intercept[RuntimeException] {
      join(false, probe, build)
    }

    assert(thrown.getCause eq closeFailure)
    assert(build.closeCalls === 1)
    assert(probe.closeCalls === 1)
  }

  test("SemiJoin validates setup before taking ownership of inputs") {
    val probe = new TrackingIterator(Seq(probeBatch(LongJ.valueOf(1) -> "one")))
    val build = new TrackingIterator(Seq(buildBatch(LongJ.valueOf(1) -> "one")))
    val operator = new SemiJoin(
      false,
      Seq(new Column("missing")).asJava,
      Seq(new Column("build_id")).asJava)

    assertThrows[IllegalArgumentException] {
      SemiJoinExecutor.execute(operator, probeSchema, buildSchema, probe, build)
    }
    assert(probe.hasNextCalls === 0)
    assert(probe.closeCalls === 0)
    assert(build.hasNextCalls === 0)
    assert(build.closeCalls === 0)
  }

  test("SemiJoin rejects a bad probe batch lazily and remains closeable") {
    val wrongSchema = new StructType().add("wrong", LongType.LONG)
    val wrongBatch = batch(
      wrongSchema,
      Seq(GenericRow.fromValues(wrongSchema, Seq(LongJ.valueOf(1)).asJava)))
    val probe = new TrackingIterator(Seq(wrongBatch))
    val output = join(
      false,
      probe,
      new TrackingIterator(Seq(buildBatch(LongJ.valueOf(1) -> "one"))))

    val error = intercept[IllegalArgumentException] {
      try output.next()
      finally output.close()
    }

    assert(error.getMessage.contains("probe batch schema"))
    assert(probe.closeCalls === 1)
  }

  private def selected(batch: FilteredColumnarBatch): Seq[(Long, String)] = {
    val rows = batch.getRows
    try rows.asScala.map(row => row.getLong(0) -> row.getString(1)).toSeq
    finally rows.close()
  }

  private def booleanVector(values: Boolean*): ColumnVector =
    new DefaultBooleanVector(values.size, Optional.empty(), values.toArray)

  private class TrackingIterator(
      batches: Seq[FilteredColumnarBatch],
      failure: Option[RuntimeException] = None,
      closeFailure: Option[RuntimeException] = None)
      extends CloseableIterator[FilteredColumnarBatch] {
    private var index = 0
    var hasNextCalls = 0
    var nextCalls = 0
    var closeCalls = 0

    override def hasNext: Boolean = {
      hasNextCalls += 1
      failure.foreach(error => throw error)
      index < batches.size
    }

    override def next(): FilteredColumnarBatch = {
      nextCalls += 1
      if (index >= batches.size) throw new NoSuchElementException
      val result = batches(index)
      index += 1
      result
    }

    override def close(): Unit = {
      closeCalls += 1
      closeFailure.foreach(error => throw error)
    }
  }
}
