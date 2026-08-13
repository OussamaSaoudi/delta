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
package io.delta.kernel.internal.plans

import java.lang.{Long => LongJ}
import java.util

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.expressions._
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.types.{LongType, StringType, StructType}
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

class PlanBuilderSuite extends AnyFunSuite {
  private val schema = new StructType()
    .add("id", LongType.LONG, false)
    .add("name", StringType.STRING)

  private def column(parts: String*): Column = new Column(parts.toArray)

  private def rows(ids: Long*): util.List[Row] = ids.map { id =>
    GenericRow.fromValues(
      schema,
      Seq(LongJ.valueOf(id), s"value-$id").asJava).asInstanceOf[Row]
  }.asJava

  private def values(ids: Long*): PlanBuilder = PlanBuilder.values(schema, rows(ids: _*))

  test("a fluent chain builds in topological order") {
    val projectedSchema = new StructType().add("id", LongType.LONG, false)
    val plan = values(1, 2)
      .filter(new Predicate("IS_NOT_NULL", column("id")))
      .project(
        new StructExpression(Seq[Expression](column("id")).asJava),
        projectedSchema)
      .aggregate(Aggregate.ungrouped(projectedSchema).max(column("id")).build())
      .build()

    assert(plan.getNodes.asScala.map(_.getOperator.getClass) === Seq(
      classOf[Values],
      classOf[Filter],
      classOf[Project],
      classOf[Aggregate]))
    assert(plan.getNodes.asScala.map(_.getInputs.asScala.toSeq) === Seq(
      Seq.empty,
      Seq(0),
      Seq(1),
      Seq(2)))
    assert(plan.getOutputSchema.fieldNames.asScala === Seq("id"))
  }

  test("shared subgraphs are emitted once and unreachable builders are omitted") {
    val source = values(1)
    source.filter(new Predicate("IS_NULL", column("name"))) // Unreachable.
    val shared = source.filter(new Predicate("IS_NOT_NULL", column("id")))
    val left = shared.filter(new Predicate("IS_NOT_NULL", column("name")))
    val right = shared.filter(new Predicate("IS_NULL", column("name")))
    val plan = PlanBuilder.unionAll(Seq(left, right).asJava).build()

    assert(plan.getNodes.size() === 5)
    assert(plan.getNodes.asScala.map(_.getInputs.asScala.toSeq) === Seq(
      Seq.empty,
      Seq(0),
      Seq(1),
      Seq(1),
      Seq(2, 3)))
  }

  test("unionAll forwards a single input unchanged") {
    val source = values(1)

    assert(PlanBuilder.unionAll(Seq(source).asJava) eq source)
  }

  test("aggregate helpers infer the input schema") {
    val grouped = values(1, 2)
      .aggregateBy(Seq(column("name")).asJava, _.max(column("id")))
      .build()
    val ungrouped = values(1, 2)
      .aggregateUngrouped(_.max(column("id")))
      .build()

    assert(grouped.getOutputSchema.fieldNames.asScala === Seq("name", "id"))
    assert(ungrouped.getOutputSchema.fieldNames.asScala === Seq("id"))
  }

  test("semi-join and anti-join preserve probe-first input order") {
    val buildSchema = new StructType().add("key", LongType.LONG, false)
    val buildRow = GenericRow.fromValues(buildSchema, Seq(LongJ.valueOf(1)).asJava)
    val build = PlanBuilder.values(buildSchema, Seq(buildRow).asJava)

    val modes = Seq(
      false -> values(1).semiJoin(build, Seq(column("id")).asJava, Seq(column("key")).asJava),
      true -> values(1).antiJoin(build, Seq(column("id")).asJava, Seq(column("key")).asJava))

    modes.foreach { case (inverted, builder) =>
      val plan = builder.build()
      val join = plan.getNodes.get(2).getOperator.asInstanceOf[SemiJoin]
      assert(plan.getNodes.get(2).getInputs.asScala === Seq(0, 1))
      assert(join.isInverted === inverted)
      assert(plan.getOutputSchema === schema)
    }
  }

  test("scan factories retain their concrete source operators") {
    val file = new ScanFile(FileStatus.of("file:///table/part-000.parquet", 10, 20))
    val parquet = PlanBuilder.scanParquet(Seq(file).asJava, Seq.empty[String].asJava, schema)
    val json = PlanBuilder.scanJson(Seq(file).asJava, Seq.empty[String].asJava, schema)

    assert(parquet.build().getNodes.get(0).getOperator.isInstanceOf[ScanParquet])
    assert(json.build().getNodes.get(0).getOperator.isInstanceOf[ScanJson])
  }

  test("empty Values remains an ordinary runnable source") {
    val plan = PlanBuilder.values(schema, util.Collections.emptyList[Row]()).build()

    assert(plan.getNodes.size() === 1)
    assert(plan.getNodes.get(0).getOperator.asInstanceOf[Values].getRows.isEmpty)
    assert(plan.getOutputSchema === schema)
  }

  test("operator validation fails at the transform call site") {
    val error = intercept[IllegalArgumentException] {
      values(1).filter(new Predicate("IS_NOT_NULL", column("missing")))
    }
    assert(error.getMessage.contains("Filter predicate"))
    assert(error.getMessage.contains("absent from schema"))

    assertThrows[IllegalArgumentException] {
      PlanBuilder.unionAll(util.Collections.emptyList[PlanBuilder]())
    }
  }
}
