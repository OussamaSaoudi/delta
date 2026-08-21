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

import java.lang.{Integer => IntegerJ, Long => LongJ}
import java.util

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.types.{LongType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class PlanSuite extends AnyFunSuite {
  private val schema = new StructType()
    .add("id", LongType.LONG, false)
    .add("name", StringType.STRING)

  private def row(id: Long, name: String): Row =
    GenericRow.fromValues(schema, Seq(LongJ.valueOf(id), name).asJava)

  private def values(ids: Long*): Values =
    new Values(schema, ids.map(id => row(id, s"value-$id")).asJava)

  private def node(operator: Operator, inputs: Int*): PlanNode =
    new PlanNode(operator, inputs.map(IntegerJ.valueOf).asJava)

  test("plan data structures defensively copy collections") {
    val mutableRows = new util.ArrayList[Row]()
    mutableRows.add(row(1, "one"))
    val source = new Values(schema, mutableRows)
    mutableRows.clear()

    val mutableNodes = new util.ArrayList[PlanNode]()
    mutableNodes.add(node(source))
    val plan = new Plan(mutableNodes)
    mutableNodes.clear()

    assert(source.getRows.size() === 1)
    assert(plan.getNodes.size() === 1)
    assertThrows[UnsupportedOperationException](source.getRows.clear())
    assertThrows[UnsupportedOperationException](plan.getNodes.clear())
  }

  test("Values accepts empty rows and enforces row schema and nullability") {
    val empty = new Values(schema, util.Collections.emptyList[Row]())
    assert(empty.getRows.isEmpty)
    assert(new Plan(Seq(node(empty)).asJava).getOutputSchema === schema)

    val otherSchema = new StructType().add("id", LongType.LONG, false)
    val mismatched = GenericRow.fromValues(otherSchema, Seq(LongJ.valueOf(1)).asJava)
    val schemaError = intercept[IllegalArgumentException] {
      new Values(schema, Seq(mismatched).asJava)
    }
    assert(schemaError.getMessage.contains("schema differs"))

    val nullId = GenericRow.fromValues(schema, Seq(null, "name").asJava)
    val nullError = intercept[IllegalArgumentException] {
      new Values(schema, Seq(nullId).asJava)
    }
    assert(nullError.getMessage.contains("non-nullable field `id`"))
  }

  test("plan rejects empty, negative, and non-topological input references") {
    assertThrows[IllegalArgumentException] {
      new Plan(util.Collections.emptyList[PlanNode]())
    }
    assertThrows[IllegalArgumentException] {
      node(UnionAll.UNION_ALL, -1)
    }

    val selfReference = intercept[IllegalArgumentException] {
      new Plan(Seq(node(values(1), 0)).asJava)
    }
    assert(selfReference.getMessage.contains("inputs must reference an earlier node"))

  }

  test("Values rejects inputs") {
    val valuesWithInput = intercept[IllegalArgumentException] {
      new Plan(Seq(node(values(1)), node(values(2), 0), node(values(3))).asJava)
    }
    assert(valuesWithInput.getMessage.contains("Values requires no inputs"))
  }
}
