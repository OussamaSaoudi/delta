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
package io.delta.spark.internal.v2.kernel

import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.execution.{PlanExecutor, PlanResultCache}
import io.delta.kernel.plans.{Aggregate => KernelAggregate}
import io.delta.kernel.plans.{Filter => KernelFilter}
import io.delta.kernel.plans.{Project => KernelProject}
import io.delta.kernel.plans.SemiJoin
import io.delta.kernel.plans.UnionAll
import io.delta.kernel.plans.Values

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.expressions.GreaterThan
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.catalyst.expressions.aggregate.MaxBy
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.CTERelationDef
import org.apache.spark.sql.catalyst.plans.logical.CTERelationRef
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.JoinHint
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.catalyst.plans.logical.WithCTE
import org.apache.spark.sql.types.{IntegerType, LongType}

class SparkPlansSuite extends SparkFunSuite {
  test("convert local values, filter, and one-expression project") {
    val id = AttributeReference("id", IntegerType, nullable = false)()
    val local = relation(id, 1, 2)
    val filtered = Filter(GreaterThan(id, Literal(1)), local)
    val projected = Project(Seq(Alias(id, "renamed")()), filtered)

    val result = SparkPlans.convert(projected)
    assert(result.isPresent)
    val project = result.get().asInstanceOf[KernelProject]
    assert(project.rowExpression().isInstanceOf[SparkOpaqueExpression])
    assert(project.input().isInstanceOf[KernelFilter])
    assert(project.input().asInstanceOf[KernelFilter].input().isInstanceOf[Values])
  }

  test("convert union without relying on shared plan identity") {
    val leftId = AttributeReference("id", IntegerType, nullable = false)()
    val rightId = AttributeReference("id", IntegerType, nullable = false)()
    val union = Union(Seq(relation(leftId, 1), relation(rightId, 2)))

    val result = SparkPlans.convert(union)
    assert(result.isPresent)
    assert(result.get().asInstanceOf[UnionAll].inputs().size() == 2)
  }

  test("reuse one kernel plan node for references to the same CTE") {
    val id = AttributeReference("id", IntegerType, nullable = false)()
    val definition = CTERelationDef(relation(id, 1, 2))
    val first = CTERelationRef(
      definition.id,
      _resolved = true,
      definition.output,
      isStreaming = false)
    val second = first.newInstance().asInstanceOf[CTERelationRef]
    val withCte = WithCTE(Union(Seq(first, second)), Seq(definition))

    val result = SparkPlans.convert(withCte)
    assert(result.isPresent)
    val inputs = result.get().asInstanceOf[UnionAll].inputs()
    assert(inputs.get(0) eq inputs.get(1))
  }

  test("convert null-safe semi join and reject nullable ordinary equality") {
    val leftId = AttributeReference("left_id", IntegerType, nullable = true)()
    val rightId = AttributeReference("right_id", IntegerType, nullable = true)()
    val left = relation(leftId, 1)
    val right = relation(rightId, 1)

    val nullSafe = Join(left, right, LeftSemi, Some(EqualNullSafe(leftId, rightId)), JoinHint.NONE)
    val converted = SparkPlans.convert(nullSafe)
    assert(converted.isPresent)
    assert(!converted.get().asInstanceOf[SemiJoin].inverted())

    val ordinary = Join(left, right, LeftSemi, Some(EqualTo(leftId, rightId)), JoinHint.NONE)
    assert(SparkPlans.convert(ordinary).isEmpty)
  }

  test("convert non-null ordinary equality and empty-key anti join") {
    val leftId = AttributeReference("left_id", IntegerType, nullable = false)()
    val rightId = AttributeReference("right_id", IntegerType, nullable = false)()
    val left = relation(leftId, 1)
    val right = relation(rightId, 1)

    val ordinary = Join(left, right, LeftSemi, Some(EqualTo(leftId, rightId)), JoinHint.NONE)
    assert(SparkPlans.convert(ordinary).isPresent)

    val emptyKey = Join(left, right, LeftAnti, None, JoinHint.NONE)
    val converted = SparkPlans.convert(emptyKey)
    assert(converted.isPresent)
    val join = converted.get().asInstanceOf[SemiJoin]
    assert(join.inverted())
    assert(join.keyTypes().isEmpty)
  }

  test("convert aggregate functions and preserve Catalyst output order") {
    val group = AttributeReference("group", IntegerType, nullable = true)()
    val value = AttributeReference("value", LongType, nullable = true)()
    val local = relation(Seq(group, value), Seq(InternalRow(1, 3L), InternalRow(1, 4L)))
    val max = complete(Max(value), "maximum")
    val maxBy = complete(MaxBy(value, group), "maximum_by")
    val count = complete(Count(Seq(value)), "count")
    val aggregate = Aggregate(Seq(group), Seq(max, Alias(group, "key")(), maxBy, count), local)

    val result = SparkPlans.convert(aggregate)
    assert(result.isPresent)
    val project = result.get().asInstanceOf[KernelProject]
    val kernelAggregate = project.input().asInstanceOf[KernelAggregate]
    assert(kernelAggregate.groupBy().size() == 1)
    assert(kernelAggregate.aggregates().size() == 3)
    assert(project.outputSchema().length() == aggregate.output.length)
  }

  test("reject an unsupported expression without retaining a convertible child") {
    val id = AttributeReference("id", IntegerType, nullable = false)()
    val child = relation(id, 1)
    val nondeterministic = Alias(org.apache.spark.sql.catalyst.expressions.Rand(1L), "random")()

    assert(SparkPlans.convert(Project(Seq(nondeterministic), child)).isEmpty)
  }

  test("execute a converted Catalyst subtree through the shared executor") {
    val id = AttributeReference("id", IntegerType, nullable = false)()
    val local = relation(id, 1, 2, 3)
    val filtered = Filter(GreaterThan(id, Literal(1)), local)
    val projected = Project(Seq(Alias(Add(id, Literal(10)), "result")()), filtered)
    val converted = SparkPlans.convert(projected)
    assert(converted.isPresent)

    val cache = new PlanResultCache(4, Long.MaxValue)
    val engine = KernelEngineFactory.withSparkExpressions(
      DefaultEngine.create(new Configuration(false)))
    val output = new PlanExecutor(engine, cache).execute(converted.get())
    val values = ArrayBuffer.empty[Int]
    try {
      while (output.hasNext) {
        val rows = output.next().getRows
        try {
          while (rows.hasNext) values += rows.next().getInt(0)
        } finally {
          rows.close()
        }
      }
    } finally {
      output.close()
      cache.close()
    }
    assert(values.toSeq == Seq(12, 13))
  }

  private def complete(
      function: org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction,
      name: String): NamedExpression = {
    Alias(
      AggregateExpression(function, Complete, isDistinct = false, None, NamedExpression.newExprId),
      name)()
  }

  private def relation(attribute: AttributeReference, values: Int*): LocalRelation =
    relation(Seq(attribute), values.map(value => InternalRow(value)))

  private def relation(
      attributes: Seq[AttributeReference],
      rows: Seq[InternalRow]): LocalRelation =
    LocalRelation(attributes, rows)
}
