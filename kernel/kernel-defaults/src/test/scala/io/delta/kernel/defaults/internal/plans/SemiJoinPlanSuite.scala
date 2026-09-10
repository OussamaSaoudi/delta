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
import io.delta.kernel.expressions.{Column, Expression, Literal, Predicate}
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.plans.{Filter, PlanNode, SemiJoin, Values}
import io.delta.kernel.types.{BinaryType, DataType, LongType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class SemiJoinPlanSuite extends AnyFunSuite with PlanExecutionSuiteBase {
  private val probeSchema = new StructType()
    .add("id", LongType.LONG)
    .add("tag", StringType.STRING)
  private val buildSchema = new StructType()
    .add("build_id", LongType.LONG)
    .add("build_tag", StringType.STRING)
  private val probeKeys = Seq[Expression](new Column("id"), new Column("tag"))
  private val buildKeys = Seq[Expression](new Column("build_id"), new Column("build_tag"))
  private val keyTypes = Seq[DataType](LongType.LONG, StringType.STRING)

  private def row(schema: StructType, values: AnyRef*): Row =
    GenericRow.fromValues(schema, values.asJava)

  private def values(schema: StructType, rows: Row*): Values =
    new Values(schema, rows.asJava)

  private def join(
      probe: PlanNode,
      build: PlanNode,
      probeKeys: Seq[Expression],
      buildKeys: Seq[Expression],
      keyTypes: Seq[DataType],
      inverted: Boolean): SemiJoin =
    new SemiJoin(
      probe,
      build,
      probeKeys.asJava,
      buildKeys.asJava,
      keyTypes.asJava,
      inverted)

  for (
    (inverted, expected) <- Seq(
      false -> Seq(1L -> "one", 1L -> null),
      true -> Seq(2L -> "two", 3L -> "three"))
  ) {
    test(s"${if (inverted) "anti" else "semi"} join uses null-safe compound keys") {
      val probe = values(
        probeSchema,
        row(probeSchema, LongJ.valueOf(1), "one"),
        row(probeSchema, LongJ.valueOf(1), null),
        row(probeSchema, LongJ.valueOf(2), "two"),
        row(probeSchema, LongJ.valueOf(3), "three"))
      val build = values(
        buildSchema,
        row(buildSchema, LongJ.valueOf(1), "one"),
        row(buildSchema, LongJ.valueOf(1), null))
      val plan = join(probe, build, probeKeys, buildKeys, keyTypes, inverted)

      checkRows(
        plan,
        expected.map { case (id, tag) =>
          row(probeSchema, LongJ.valueOf(id), tag)
        })
    }
  }

  test("honor selections on both inputs") {
    val probe = new Filter(
      values(
        probeSchema,
        row(probeSchema, LongJ.valueOf(1), "one"),
        row(probeSchema, LongJ.valueOf(2), "two")),
      new Predicate(">", new Column("id"), Literal.ofLong(1)))
    val build = new Filter(
      values(
        buildSchema,
        row(buildSchema, LongJ.valueOf(1), "one"),
        row(buildSchema, LongJ.valueOf(2), "two")),
      new Predicate(">", new Column("build_id"), Literal.ofLong(1)))

    checkRows(
      join(probe, build, probeKeys, buildKeys, keyTypes, inverted = false),
      Seq(row(probeSchema, LongJ.valueOf(2), "two")))
  }

  for (
    (inverted, expected) <- Seq(
      false -> Seq.empty[Row],
      true -> Seq(row(probeSchema, LongJ.valueOf(1), "one")))
  ) {
    test(s"${if (inverted) "anti" else "semi"} join handles an empty build") {
      val probe = values(probeSchema, row(probeSchema, LongJ.valueOf(1), "one"))
      val build = values(buildSchema)

      checkRows(join(probe, build, probeKeys, buildKeys, keyTypes, inverted), expected)
    }
  }

  test("empty join keys distinguish empty and non-empty builds") {
    val expected = Seq(
      row(probeSchema, LongJ.valueOf(1), "one"),
      row(probeSchema, LongJ.valueOf(2), "two"))
    val probe = new Values(probeSchema, expected.asJava)
    val nonEmptyBuild = values(buildSchema, row(buildSchema, LongJ.valueOf(9), "nine"))
    val emptyBuild = values(buildSchema)

    checkRows(
      join(probe, nonEmptyBuild, Seq.empty, Seq.empty, Seq.empty, inverted = false),
      expected)
    checkRows(
      join(probe, emptyBuild, Seq.empty, Seq.empty, Seq.empty, inverted = true),
      expected)
  }

  test("compare binary keys by content") {
    val probeType = new StructType().add("key", BinaryType.BINARY)
    val buildType = new StructType().add("other", BinaryType.BINARY)
    val probe = values(
      probeType,
      row(probeType, Array[Byte](1, 2)),
      row(probeType, Array[Byte](3)))
    val build = values(buildType, row(buildType, Array[Byte](1, 2)))

    checkRows(
      join(
        probe,
        build,
        Seq(new Column("key")),
        Seq(new Column("other")),
        Seq(BinaryType.BINARY),
        inverted = false),
      Seq(row(probeType, Array[Byte](1, 2))))
  }

  test("reject incompatible key types when execution is wired") {
    val wrongBuildSchema = new StructType().add("build_id", StringType.STRING)
    val plan = join(
      values(probeSchema),
      values(wrongBuildSchema),
      Seq(new Column("id")),
      Seq(new Column("build_id")),
      Seq(LongType.LONG),
      inverted = false)

    intercept[UnsupportedOperationException] {
      checkRows(plan, Seq.empty)
    }
  }
}
