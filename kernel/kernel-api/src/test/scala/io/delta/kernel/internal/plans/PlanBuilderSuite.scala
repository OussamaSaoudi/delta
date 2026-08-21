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
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.types.{LongType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class PlanBuilderSuite extends AnyFunSuite {
  private val schema = new StructType()
    .add("id", LongType.LONG, false)
    .add("name", StringType.STRING)

  private def values(ids: Long*): PlanBuilder = {
    val rows: util.List[Row] = ids.map { id =>
      GenericRow.fromValues(schema, Seq(LongJ.valueOf(id), s"value-$id").asJava)
    }.asJava
    PlanBuilder.values(schema, rows)
  }

  test("values build a source plan") {
    val plan = values(1, 2).build()

    assert(plan.getNodes.asScala.map(_.getOperator.getClass) === Seq(classOf[Values]))
    assert(plan.getNodes.get(0).getInputs.isEmpty)
    assert(plan.getOutputSchema(0) === schema)
    assert(plan.getOutputSchema === schema)
  }

  test("union builds in topological order") {
    val plan = PlanBuilder.unionAll(Seq(values(1, 2), values(3)).asJava).build()

    assert(plan.getNodes.asScala.map(_.getOperator.getClass) === Seq(
      classOf[Values], classOf[Values], classOf[UnionAll]))
    assert(plan.getNodes.asScala.map(_.getInputs.asScala.toSeq) === Seq(
      Seq.empty, Seq.empty, Seq(0, 1)))
    assert(plan.getOutputSchema === schema)
  }

  test("shared inputs are emitted once") {
    val source = values(1)
    val plan = PlanBuilder.unionAll(Seq(source, source).asJava).build()

    assert(plan.getNodes.size() === 2)
    assert(plan.getNodes.get(1).getInputs.asScala === Seq(0, 0))
  }

  test("single-input union returns its input") {
    val source = values(1)

    assert(PlanBuilder.unionAll(Seq(source).asJava) eq source)
  }

  test("empty values remains a plan node") {
    val plan = PlanBuilder.values(schema, util.Collections.emptyList[Row]()).build()

    assert(plan.getNodes.size() === 1)
    assert(plan.getOutputSchema === schema)
  }
}
