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

import io.delta.kernel.data.{FilteredColumnarBatch, Row, VariantValue}
import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.plans.SemiJoin
import io.delta.kernel.types.{BinaryType, IntervalDayTimeType, IntervalYearMonthType, LongType, StringType, StructType, VariantType}
import io.delta.kernel.utils.CloseableIterator

import PlanTestUtils._
import org.scalatest.funsuite.AnyFunSuite

class SemiJoinExecutorSuite extends AnyFunSuite {
  private val probeSchema = new StructType()
    .add("id", LongType.LONG)
    .add("tag", StringType.STRING)
  private val buildSchema = new StructType()
    .add("build_id", LongType.LONG)
    .add("build_tag", StringType.STRING)

  private def probeRow(id: java.lang.Long, tag: String): Row =
    row(probeSchema, id, tag)

  private def buildRow(id: java.lang.Long, tag: String): Row =
    row(buildSchema, id, tag)

  private def batch(schema: StructType, rows: Seq[Row]): FilteredColumnarBatch =
    PlanTestUtils.batch(schema, rows)

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
    val selectedBuild = selectedBatch(rawBuild.getData, true, false, true)
    val first = probeBatch(LongJ.valueOf(1) -> "one", LongJ.valueOf(2) -> "two")
    val rawSecond = probeBatch(LongJ.valueOf(3) -> "three", LongJ.valueOf(1) -> "one")
    val second = selectedBatch(
      rawSecond.getData,
      Seq(false, true).map(Boolean.box),
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
        new TrackingIterator[FilteredColumnarBatch](Seq.empty))
      try assert(selected(output.next()) === expected)
      finally output.close()
    }
  }

  test("SemiJoin compares binary keys by content") {
    val probeBinarySchema = new StructType().add("key", BinaryType.BINARY)
    val buildBinarySchema = new StructType().add("other", BinaryType.BINARY)
    def binaryBatch(schema: StructType, values: Seq[Array[Byte]]) =
      batch(schema, values.map(value => row(schema, value)))
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

  test("SemiJoin compares Variant keys by encoded value and metadata") {
    val variantProbeSchema = new StructType().add("key", VariantType.VARIANT)
    val variantBuildSchema = new StructType().add("other", VariantType.VARIANT)
    val matched = new VariantValue(Array[Byte](1), Array[Byte](10))
    val unmatched = new VariantValue(Array[Byte](2), Array[Byte](20))
    val operator = new SemiJoin(
      false,
      Seq(new Column("key")).asJava,
      Seq(new Column("other")).asJava)
    val output = SemiJoinExecutor.execute(
      operator,
      variantProbeSchema,
      variantBuildSchema,
      new TrackingIterator(Seq(batch(
        variantProbeSchema,
        Seq(row(variantProbeSchema, matched), row(variantProbeSchema, unmatched))))),
      new TrackingIterator(Seq(batch(
        variantBuildSchema,
        Seq(row(variantBuildSchema, new VariantValue(Array[Byte](1), Array[Byte](10))))))))

    try assert(rows(output.next()).map(_.getVariant(0)) === Seq(matched))
    finally output.close()
  }

  test("SemiJoin canonicalizes both interval families without physical type erasure") {
    val yearMonth = IntervalYearMonthType.INTERVAL_YEAR_MONTH
    val dayTime = IntervalDayTimeType.INTERVAL_DAY_TIME
    val intervalProbeSchema = new StructType()
      .add("ym", yearMonth)
      .add("dt", dayTime)
    val intervalBuildSchema = new StructType()
      .add("buildYm", yearMonth)
      .add("buildDt", dayTime)
    val operator = new SemiJoin(
      false,
      Seq(new Column("ym"), new Column("dt")).asJava,
      Seq(new Column("buildYm"), new Column("buildDt")).asJava)
    val probeBatch = batch(
      intervalProbeSchema,
      Seq(
        row(intervalProbeSchema, Int.box(-13), Long.box(-5L)),
        row(intervalProbeSchema, Int.box(30), Long.box(7L))))
    val buildBatch = batch(
      intervalBuildSchema,
      Seq(row(intervalBuildSchema, Int.box(-13), Long.box(-5L))))
    val output = SemiJoinExecutor.execute(
      operator,
      intervalProbeSchema,
      intervalBuildSchema,
      new TrackingIterator(Seq(probeBatch)),
      new TrackingIterator(Seq(buildBatch)))

    try {
      val matched = rows(output.next())
      assert(matched.map(_.getIntervalYearMonth(0)) === Seq(-13))
      assert(matched.map(_.getIntervalDayTime(1)) === Seq(-5L))
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
    val build = new TrackingIterator[FilteredColumnarBatch](
      Seq.empty,
      hasNextFailure = Some(failure))

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
      Seq(row(wrongSchema, LongJ.valueOf(1))))
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
    rows(batch).map(row => row.getLong(0) -> row.getString(1))
  }
}
