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
package io.delta.kernel.plans

import java.lang.{Long => LongJ}
import java.util

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.types.{IntegerType, LongType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class PlanSuite extends AnyFunSuite {
  private val schema = new StructType()
    .add("id", LongType.LONG, false)
    .add("name", StringType.STRING)

  private def row(id: Long): Row =
    GenericRow.fromValues(schema, Seq(LongJ.valueOf(id), s"value-$id").asJava)

  private def values(ids: Long*): Values =
    new Values(schema, ids.map(row).asJava)

  test("plan nodes defensively copy payloads and expose children") {
    val rows = new util.ArrayList[Row](Seq(row(1)).asJava)
    val source = new Values(schema, rows)
    rows.clear()

    val inputs = new util.ArrayList[PlanNode](Seq(source, source).asJava)
    val union = new UnionAll(inputs)
    inputs.clear()

    assert(source.ownedRows().size() === 1)
    assert(union.children().asScala === Seq(source, source))
    assert(union.children().get(0) eq union.children().get(1))
    assertThrows[UnsupportedOperationException](source.ownedRows().clear())
    assertThrows[UnsupportedOperationException](union.children().clear())
  }

  test("structural equality is independent of execution identity") {
    val first = new UnionAll(Seq[PlanNode](values(1), values(2)).asJava)
    val equal = new UnionAll(Seq[PlanNode](values(1), values(2)).asJava)
    val different = new UnionAll(Seq[PlanNode](values(1), values(3)).asJava)

    assert(first === equal)
    assert(first.hashCode() === equal.hashCode())
    assert(first !== different)
    assert(!(first.inputs().get(0) eq equal.inputs().get(0)))
  }

  test("Values validates schema and nullability") {
    val otherSchema = new StructType().add("id", LongType.LONG, false)
    val mismatched = GenericRow.fromValues(otherSchema, Seq(LongJ.valueOf(1)).asJava)
    assertThrows[IllegalArgumentException](new Values(schema, Seq(mismatched).asJava))

    val nullId = GenericRow.fromValues(schema, Seq(null, "name").asJava)
    assertThrows[IllegalArgumentException](new Values(schema, Seq(nullId).asJava))
  }

  test("UnionAll requires equal input schemas") {
    assertThrows[IllegalArgumentException](new UnionAll(util.Collections.emptyList()))

    val otherSchema = new StructType().add("other", IntegerType.INTEGER)
    val otherRow = GenericRow.fromValues(otherSchema, Seq(Int.box(1)).asJava)
    val other = new Values(otherSchema, Seq(otherRow).asJava)
    assertThrows[IllegalArgumentException](new UnionAll(Seq[PlanNode](values(1), other).asJava))
  }
}
