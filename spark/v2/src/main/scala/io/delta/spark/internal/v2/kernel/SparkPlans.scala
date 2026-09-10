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

import java.util.{ArrayList, IdentityHashMap, Optional => JOptional}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

import io.delta.kernel.data.Row
import io.delta.kernel.expressions.{Expression => KernelExpression}
import io.delta.kernel.internal.util.RowKernels
import io.delta.kernel.plans.{Aggregate => KernelAggregate, _}
import io.delta.kernel.types.{DataType => KernelDataType}
import io.delta.spark.internal.v2.utils.SchemaUtils

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.BindReferences
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.KnownNullable
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.catalyst.expressions.aggregate.MaxBy
import org.apache.spark.sql.catalyst.expressions.aggregate.Min
import org.apache.spark.sql.catalyst.expressions.aggregate.MinBy
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate => SparkAggregate}
import org.apache.spark.sql.catalyst.plans.logical.{Filter => SparkFilter}
import org.apache.spark.sql.catalyst.plans.logical.{Project => SparkProject}
import org.apache.spark.sql.catalyst.plans.logical.CTERelationDef
import org.apache.spark.sql.catalyst.plans.logical.CTERelationRef
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.catalyst.plans.logical.WithCTE
import org.apache.spark.sql.types.{DataType => SparkDataType}
import org.apache.spark.sql.types.{StructField => SparkStructField}
import org.apache.spark.sql.types.{StructType => SparkStructType}
import org.apache.spark.sql.types.LongType

/** Converts one wholly supported Catalyst subtree into a Kernel plan. */
object SparkPlans {

  /** Returns empty rather than leaving unsupported Catalyst work inside a Kernel subtree. */
  def convert(plan: LogicalPlan): JOptional[PlanNode] = {
    if (plan == null) return JOptional.empty()
    try {
      new Converter().convert(plan) match {
        case Some(converted) => JOptional.of(converted)
        case None => JOptional.empty()
      }
    } catch {
      case NonFatal(_) => JOptional.empty()
    }
  }

  private final class Converter {
    private val converted = new IdentityHashMap[LogicalPlan, PlanNode]
    private val ctes = scala.collection.mutable.Map.empty[Long, (PlanNode, Seq[Attribute])]

    def convert(plan: LogicalPlan): Option[PlanNode] = {
      if (!plan.resolved) return None
      Option(converted.get(plan)).orElse {
        val result = plan match {
          case local: LocalRelation => convertLocal(local)
          case SparkFilter(condition, child) =>
            for {
              input <- convert(child)
              predicate <- boundPredicate(condition, child.output)
            } yield new Filter(input, predicate)
          case SparkProject(projectList, child) =>
            for {
              input <- convert(child)
              expression <- boundStruct(projectList, plan.output, child.output)
              schema <- kernelSchema(plan.schema)
            } yield new Project(input, expression, schema)
          case union: Union => convertUnion(union)
          case join: Join => convertJoin(join)
          case aggregate: SparkAggregate => convertAggregate(aggregate)
          case withCte: WithCTE => convertWithCte(withCte)
          case definition: CTERelationDef => convertCteDefinition(definition)
          case reference: CTERelationRef => convertCteReference(reference)
          case _ => None
        }
        result.foreach(converted.put(plan, _))
        result
      }
    }

    private def convertWithCte(withCte: WithCTE): Option[PlanNode] =
      sequence(withCte.cteDefs.map(convertCteDefinition)).flatMap(_ => convert(withCte.plan))

    private def convertCteDefinition(definition: CTERelationDef): Option[PlanNode] = {
      convert(definition.child).map { plan =>
        ctes.put(definition.id, plan -> definition.output)
        plan
      }
    }

    private def convertCteReference(reference: CTERelationRef): Option[PlanNode] = {
      if (reference.recursive) return None
      for {
        definition <- ctes.get(reference.cteId)
        schema <- kernelSchema(reference.schema)
        aligned <- align(definition._1, definition._2, reference.output, schema)
      } yield aligned
    }

    private def convertLocal(local: LocalRelation): Option[PlanNode] = {
      if (local.isStreaming) return None
      kernelSchema(local.schema).map { schema =>
        val data = local.data.toArray
        val batch = new SparkRowArrayBatch(schema, local.schema, data, data.length)
        val rows = new ArrayList[Row](data.length)
        val input = batch.getRows
        try {
          while (input.hasNext) rows.add(RowKernels.materialize(input.next()))
        } finally {
          input.close()
        }
        new Values(schema, rows)
      }
    }

    private def convertUnion(union: Union): Option[PlanNode] = {
      if (union.children.isEmpty) return None
      for {
        schema <- kernelSchema(union.schema)
        inputs <- sequence(union.children.map(convert))
        aligned <- sequence(inputs.zip(union.children).map { case (input, child) =>
          align(input, child.output, union.output, schema)
        })
      } yield new UnionAll(aligned.asJava)
    }

    private def align(
        input: PlanNode,
        inputOutput: Seq[Attribute],
        output: Seq[Attribute],
        outputSchema: io.delta.kernel.types.StructType): Option[PlanNode] = {
      if (
        inputOutput.length != output.length ||
        !inputOutput.zip(output).forall { case (left, right) => left.dataType == right.dataType }
      ) {
        return None
      }
      if (input.outputSchema == outputSchema) return Some(input)

      val values = inputOutput.zip(output).zipWithIndex.map { case ((source, target), ordinal) =>
        val reference = BoundReference(ordinal, source.dataType, source.nullable)
        if (target.nullable && !source.nullable) KnownNullable(reference) else reference
      }
      opaqueStruct(values, output).map(new Project(input, _, outputSchema))
    }

    private def convertJoin(join: Join): Option[PlanNode] = {
      val inverted = join.joinType match {
        case LeftSemi => false
        case LeftAnti => true
        case _ => return None
      }
      val keys = join.condition match {
        case Some(condition) => sequence(conjuncts(condition).map(joinKey(_, join)))
        case None => Some(Seq.empty)
      }

      for {
        probe <- convert(join.left)
        build <- convert(join.right)
        convertedKeys <- keys
      } yield new SemiJoin(
        probe,
        build,
        convertedKeys.map(_._1).asJava,
        convertedKeys.map(_._2).asJava,
        convertedKeys.map(_._3).asJava,
        inverted)
    }

    private def joinKey(
        condition: Expression,
        join: Join): Option[(KernelExpression, KernelExpression, KernelDataType)] = {
      val operands = condition match {
        case EqualNullSafe(left, right) => Some(left -> right)
        case EqualTo(left, right) if !left.nullable && !right.nullable => Some(left -> right)
        case _ => None
      }
      operands.flatMap { case (first, second) =>
        val oriented = if (onSide(first, join.left) && onSide(second, join.right)) {
          Some(first -> second)
        } else if (onSide(second, join.left) && onSide(first, join.right)) {
          Some(second -> first)
        } else {
          None
        }
        oriented.flatMap { case (left, right) =>
          if (left.dataType != right.dataType) return None
          for {
            probe <- boundExpression(left, join.left.output)
            build <- boundExpression(right, join.right.output)
            dataType <- kernelType(left.dataType)
          } yield (probe, build, dataType)
        }
      }
    }

    private def onSide(expression: Expression, plan: LogicalPlan): Boolean =
      expression.references.nonEmpty && expression.references.subsetOf(plan.outputSet)

    private def convertAggregate(aggregate: SparkAggregate): Option[PlanNode] = {
      val functions = ArrayBuffer.empty[Agg]
      val outputOrdinals = ArrayBuffer.empty[Int]
      val groupCount = aggregate.groupingExpressions.length

      aggregate.aggregateExpressions.foreach { named =>
        val expression = stripAlias(named)
        val groupOrdinal = aggregate.groupingExpressions.indexWhere(_.semanticEquals(expression))
        if (groupOrdinal >= 0) {
          outputOrdinals += groupOrdinal
        } else {
          convertAggregateExpression(expression, aggregate.child.output) match {
            case Some(function) =>
              outputOrdinals += groupCount + functions.length
              functions += function
            case None => return None
          }
        }
      }

      for {
        input <- convert(aggregate.child)
        groups <- sequence(aggregate.groupingExpressions.map { expression =>
          boundExpression(expression, aggregate.child.output)
        })
        internalSparkSchema = aggregateSchema(aggregate, functions.length)
        internalSchema <- kernelSchema(internalSparkSchema)
        resultSchema <- kernelSchema(aggregate.schema)
        resultExpression <- opaqueStruct(
          outputOrdinals.map { ordinal =>
            BoundReference(
              ordinal,
              internalSparkSchema.fields(ordinal).dataType,
              internalSparkSchema.fields(ordinal).nullable)
          }.toSeq,
          aggregate.output)
      } yield {
        val kernelAggregate =
          new KernelAggregate(
            input,
            groups.map(expression => expression: KernelExpression).asJava,
            functions.asJava,
            internalSchema)
        new Project(kernelAggregate, resultExpression, resultSchema)
      }
    }

    private def aggregateSchema(
        aggregate: SparkAggregate,
        functionCount: Int): SparkStructType = {
      val groups = aggregate.groupingExpressions.zipWithIndex.map { case (expression, ordinal) =>
        SparkStructField(s"_group_$ordinal", expression.dataType, expression.nullable)
      }
      val functions = aggregate.aggregateExpressions.iterator
        .map(stripAlias)
        .collect { case expression: AggregateExpression => expression }
        .take(functionCount)
        .zipWithIndex
        .map { case (expression, ordinal) =>
          SparkStructField(s"_aggregate_$ordinal", expression.dataType, expression.nullable)
        }
        .toSeq
      SparkStructType(groups ++ functions)
    }

    private def convertAggregateExpression(
        expression: Expression,
        input: Seq[Attribute]): Option[Agg] = expression match {
      case aggregate: AggregateExpression
          if aggregate.mode == Complete && !aggregate.isDistinct && aggregate.filter.isEmpty =>
        aggregate.aggregateFunction match {
          case min: Min => minMax(min.child, input, isMax = false)
          case max: Max => minMax(max.child, input, isMax = true)
          case minBy: MinBy => nonNullBy(minBy.valueExpr, minBy.orderingExpr, input, isMax = false)
          case maxBy: MaxBy => nonNullBy(maxBy.valueExpr, maxBy.orderingExpr, input, isMax = true)
          case count: Count => convertCount(count, input)
          case sum: Sum => convertSum(sum, input)
          case _ => None
        }
      case _ => None
    }

    private def minMax(
        value: Expression,
        input: Seq[Attribute],
        isMax: Boolean): Option[Agg] = for {
      expression <- boundExpression(value, input)
      dataType <- kernelType(value.dataType)
      _ <- orderable(dataType)
    } yield if (isMax) Agg.max(expression, dataType) else Agg.min(expression, dataType)

    private def nonNullBy(
        value: Expression,
        key: Expression,
        input: Seq[Attribute],
        isMax: Boolean): Option[Agg] = for {
      valueExpression <- boundExpression(value, input)
      keyExpression <- boundExpression(key, input)
      valueType <- kernelType(value.dataType)
      keyType <- kernelType(key.dataType)
      _ <- orderable(keyType)
    } yield {
      if (isMax) {
        Agg.maxNonNullBy(
          valueExpression,
          valueType,
          keyExpression,
          keyType,
          keyExpression,
          keyType)
      } else {
        Agg.minNonNullBy(
          valueExpression,
          valueType,
          keyExpression,
          keyType,
          keyExpression,
          keyType)
      }
    }

    private def convertCount(count: Count, input: Seq[Attribute]): Option[Agg] = {
      if (count.children.isEmpty || count.children.forall(!_.nullable)) {
        Some(Agg.countStar())
      } else if (count.children.length == 1) {
        for {
          expression <- boundExpression(count.children.head, input)
          dataType <- kernelType(count.children.head.dataType)
        } yield Agg.count(expression, dataType)
      } else {
        None
      }
    }

    private def convertSum(sum: Sum, input: Seq[Attribute]): Option[Agg] = {
      if (sum.child.dataType != LongType || sum.dataType != LongType || !isChecked(sum)) return None
      boundExpression(sum.child, input).map(Agg.sum)
    }

    /** Sum moved evalMode under NumericEvalContext in Spark 4.1. */
    private def isChecked(sum: Sum): Boolean = {
      val direct = Try(sum.getClass.getMethod("evalMode").invoke(sum))
      val mode = direct.orElse(Try {
        val context = sum.getClass.getMethod("evalContext").invoke(sum)
        context.getClass.getMethod("evalMode").invoke(context)
      })
      mode.toOption.exists(_.toString == "ANSI")
    }

    private def stripAlias(expression: NamedExpression): Expression = expression match {
      case alias: Alias => alias.child
      case other => other
    }

    private def conjuncts(expression: Expression): Seq[Expression] = expression match {
      case And(left, right) => conjuncts(left) ++ conjuncts(right)
      case other => Seq(other)
    }

    private def boundExpression(
        expression: Expression,
        input: Seq[Attribute]): Option[SparkOpaqueExpression] = {
      if (!expression.resolved || !expression.deterministic) return None
      Some(new SparkOpaqueExpression(BindReferences.bindReference(expression, input)))
    }

    private def boundPredicate(
        expression: Expression,
        input: Seq[Attribute]): Option[SparkOpaquePredicate] = {
      if (!expression.resolved || !expression.deterministic) return None
      Some(new SparkOpaquePredicate(BindReferences.bindReference(expression, input)))
    }

    private def boundStruct(
        values: Seq[Expression],
        output: Seq[Attribute],
        input: Seq[Attribute]): Option[SparkOpaqueExpression] =
      opaqueStruct(values, output).flatMap(expression =>
        boundExpression(expression.catalyst(), input))

    private def opaqueStruct(
        values: Seq[Expression],
        output: Seq[Attribute]): Option[SparkOpaqueExpression] = {
      if (values.length != output.length || values.exists(e => !e.resolved || !e.deterministic)) {
        return None
      }
      val fields = values.zip(output).flatMap { case (value, field) =>
        val alias = Alias(value, field.name)(
          field.exprId,
          field.qualifier,
          Some(field.metadata),
          Seq.empty)
        Seq(Literal(field.name), alias)
      }
      Some(new SparkOpaqueExpression(CreateNamedStruct(fields)))
    }

    private def kernelSchema(schema: SparkStructType)
        : Option[io.delta.kernel.types.StructType] =
      Try(SchemaUtils.convertSparkSchemaToKernelSchema(schema)).toOption

    private def kernelType(dataType: SparkDataType): Option[KernelDataType] =
      Try(SchemaUtils.convertSparkDataTypeToKernelDataType(dataType)).toOption

    private def orderable(dataType: KernelDataType): Option[Unit] =
      Try(RowKernels.requireOrderable(dataType)).toOption

    private def sequence[T](values: Seq[Option[T]]): Option[Seq[T]] = {
      val result = ArrayBuffer.empty[T]
      values.foreach {
        case Some(value) => result += value
        case None => return None
      }
      Some(result.toSeq)
    }
  }
}
