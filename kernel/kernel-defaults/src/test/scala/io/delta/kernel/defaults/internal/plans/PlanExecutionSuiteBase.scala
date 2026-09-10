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

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.defaults.engine.DefaultExpressionHandler
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestRow}
import io.delta.kernel.engine.Engine
import io.delta.kernel.execution.{PlanExecutor, PlanResultCache}
import io.delta.kernel.internal.util.{RowKernels, Utils}
import io.delta.kernel.plans.PlanNode
import io.delta.kernel.test.MockEngineUtils

import org.scalatest.Assertions.assert

/** Test helpers for executing a passive plan and checking its logical rows. */
private[plans] trait PlanExecutionSuiteBase extends MockEngineUtils with ExpressionTestUtils {
  protected final def checkRows(plan: PlanNode, expected: Seq[Row]): Unit = {
    checkRows(plan, defaultEngine(), expected)
  }

  protected final def checkRows(plan: PlanNode, engine: Engine, expected: Seq[Row]): Unit = {
    val actual = execute(plan, engine)

    assert(actual.map(_.getSchema) == expected.map(_.getSchema))
    assert(actual.map(logicalRow) == expected.map(logicalRow))
  }

  protected final def checkRowsUnordered(plan: PlanNode, expected: Seq[Row]): Unit = {
    val actual = execute(plan, defaultEngine())

    assert(actual.map(_.getSchema) == expected.map(_.getSchema))
    def counts(rows: Seq[Row]) = rows.map(logicalRow).groupBy(identity).map {
      case (value, copies) => value -> copies.size
    }
    assert(counts(actual) == counts(expected))
  }

  private def execute(plan: PlanNode, engine: Engine): Seq[Row] = {
    val cache = new PlanResultCache(1, Long.MaxValue)
    try {
      val batches = new PlanExecutor(engine, cache).execute(plan)
      Utils.intoRows(batches)
        .map(row => RowKernels.materialize(row))
        .toInMemoryList
        .asScala
        .toSeq
    } finally {
      cache.close()
    }
  }

  private def defaultEngine(): Engine = {
    mockEngine(expressionHandler = new DefaultExpressionHandler())
  }

  private def logicalRow(row: Row): Seq[Any] = TestRow(row).toSeq.map(logicalValue)

  private def logicalValue(value: Any): Any = value match {
    case bytes: Array[Byte] => bytes.toSeq
    case nested: TestRow => nested.toSeq.map(logicalValue)
    case values: Seq[_] => values.map(logicalValue)
    case values: Map[_, _] => values.map { case (key, entry) =>
        logicalValue(key) -> logicalValue(entry)
      }
    case scalar => scalar
  }
}
