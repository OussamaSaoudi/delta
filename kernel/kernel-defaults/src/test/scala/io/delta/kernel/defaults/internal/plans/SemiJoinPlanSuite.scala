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

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.expressions.{Column, Literal, Predicate}
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.plans.PlanBuilder
import io.delta.kernel.types.{BinaryType, LongType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class SemiJoinPlanSuite extends AnyFunSuite with PlanExecutionSuiteBase {
  private val probeSchema = new StructType()
    .add("id", LongType.LONG)
    .add("tag", StringType.STRING)
  private val buildSchema = new StructType()
    .add("build_id", LongType.LONG)
    .add("build_tag", StringType.STRING)
  private val probeKeys = Seq(new Column("id"), new Column("tag")).asJava
  private val buildKeys = Seq(new Column("build_id"), new Column("build_tag")).asJava

  private def row(schema: StructType, values: AnyRef*): Row =
    GenericRow.fromValues(schema, values.asJava)

  private def values(schema: StructType, rows: Seq[Row]): PlanBuilder =
    PlanBuilder.values(schema, rows.asJava)

  for ((inverted, expected) <- Seq(
      false -> Seq(1L -> "one", 1L -> null),
      true -> Seq(2L -> "two", 3L -> "three"))) {
    test(s"${if (inverted) "anti" else "semi"} join uses null-safe compound keys") {
      val probe = values(probeSchema, Seq(
        row(probeSchema, LongJ.valueOf(1), "one"),
        row(probeSchema, LongJ.valueOf(1), null),
        row(probeSchema, LongJ.valueOf(2), "two"),
        row(probeSchema, LongJ.valueOf(3), "three")))
      val build = values(buildSchema, Seq(
        row(buildSchema, LongJ.valueOf(1), "one"),
        row(buildSchema, LongJ.valueOf(1), null)))
      val plan = if (inverted) {
        probe.antiJoin(build, probeKeys, buildKeys)
      } else {
        probe.semiJoin(build, probeKeys, buildKeys)
      }

      checkRows(plan, expected.map { case (id, tag) =>
        row(probeSchema, LongJ.valueOf(id), tag)
      })
    }
  }

  test("honor selections on both inputs") {
    val probe = values(probeSchema, Seq(
      row(probeSchema, LongJ.valueOf(1), "one"),
      row(probeSchema, LongJ.valueOf(2), "two")))
      .filter(new Predicate(">", new Column("id"), Literal.ofLong(1)))
    val build = values(buildSchema, Seq(
      row(buildSchema, LongJ.valueOf(1), "one"),
      row(buildSchema, LongJ.valueOf(2), "two")))
      .filter(new Predicate(">", new Column("build_id"), Literal.ofLong(1)))

    checkRows(
      probe.semiJoin(build, probeKeys, buildKeys),
      Seq(row(probeSchema, LongJ.valueOf(2), "two")))
  }

  for ((inverted, expected) <- Seq(false -> Seq.empty[Row], true -> Seq(
      row(probeSchema, LongJ.valueOf(1), "one")))) {
    test(s"${if (inverted) "anti" else "semi"} join handles an empty build") {
      val probe = values(
        probeSchema,
        Seq(row(probeSchema, LongJ.valueOf(1), "one")))
      val build = values(buildSchema, Seq.empty[Row])
      val plan = if (inverted) {
        probe.antiJoin(build, probeKeys, buildKeys)
      } else {
        probe.semiJoin(build, probeKeys, buildKeys)
      }

      checkRows(plan, expected)
    }
  }

  test("compare binary keys by content") {
    val probeType = new StructType().add("key", BinaryType.BINARY)
    val buildType = new StructType().add("other", BinaryType.BINARY)
    val probe = values(probeType, Seq(
      row(probeType, Array[Byte](1, 2)),
      row(probeType, Array[Byte](3))))
    val build = values(buildType, Seq(row(buildType, Array[Byte](1, 2))))

    checkRows(
      probe.semiJoin(
        build,
        Seq(new Column("key")).asJava,
        Seq(new Column("other")).asJava),
      Seq(row(probeType, Array[Byte](1, 2))))
  }

  test("reject incompatible key types while building") {
    val wrongBuildSchema = new StructType().add("build_id", StringType.STRING)
    val error = intercept[IllegalArgumentException] {
      values(probeSchema, Seq.empty[Row]).semiJoin(
        values(wrongBuildSchema, Seq.empty[Row]),
        Seq(new Column("id")).asJava,
        Seq(new Column("build_id")).asJava)
    }

    assert(error.getMessage.contains("incompatible types"))
  }
}
