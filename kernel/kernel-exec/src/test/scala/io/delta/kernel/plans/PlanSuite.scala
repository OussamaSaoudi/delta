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
import java.util.Optional

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.plans.PlanNode.{FileScan, UnionAll, Values}
import io.delta.kernel.types.{IntegerType, LongType, StringType, StructType}
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

final class PlanSuite extends AnyFunSuite {
  private val schema = new StructType()
    .add("id", LongType.LONG, false)
    .add("name", StringType.STRING)

  private def row(id: Long): Row =
    genericRow(schema, LongJ.valueOf(id), s"value-$id")

  private def genericRow(rowSchema: StructType, values: AnyRef*): Row = {
    val ordinalToValue = new util.HashMap[Integer, Object]()
    values.zipWithIndex.foreach { case (value, ordinal) =>
      ordinalToValue.put(Int.box(ordinal), value)
    }
    new GenericRow(rowSchema, ordinalToValue)
  }

  private def values(ids: Long*): Values =
    new Values(schema, ids.map(row).asJava)

  test("plan nodes defensively copy payloads") {
    val rows = new util.ArrayList[Row](Seq(row(1)).asJava)
    val source = new Values(schema, rows)
    rows.clear()

    val inputs = new util.ArrayList[PlanNode](Seq(source, source).asJava)
    val union = new UnionAll(inputs)
    inputs.clear()

    assert(source.ownedRows().size() === 1)
    assert(union.inputs().asScala === Seq(source, source))
    assert(union.inputs().get(0) eq union.inputs().get(1))
    assertThrows[UnsupportedOperationException](source.ownedRows().clear())
    assertThrows[UnsupportedOperationException](union.inputs().clear())
  }

  test("Values validates schema and nullability") {
    val otherSchema = new StructType().add("id", LongType.LONG, false)
    val mismatched = genericRow(otherSchema, LongJ.valueOf(1))
    assertThrows[IllegalArgumentException](new Values(schema, Seq(mismatched).asJava))

    val nullId = genericRow(schema, null, "name")
    assertThrows[IllegalArgumentException](new Values(schema, Seq(nullId).asJava))
  }

  test("UnionAll requires equal input schemas") {
    assertThrows[IllegalArgumentException](new UnionAll(util.Collections.emptyList()))

    val otherSchema = new StructType().add("other", IntegerType.INTEGER)
    val otherRow = genericRow(otherSchema, Int.box(1))
    val other = new Values(otherSchema, Seq(otherRow).asJava)
    assertThrows[IllegalArgumentException](new UnionAll(Seq[PlanNode](values(1), other).asJava))
  }

  test("FileScan rejects duplicate paths") {
    val constants = new GenericRow(new StructType(), util.Collections.emptyMap())
    val file = new ScanFile(FileStatus.of("file:/scan.json", 1, 0), constants, Optional.empty())
    assertThrows[IllegalArgumentException](FileScan.json(
      Seq(file, file).asJava,
      Optional.empty(),
      Seq.empty.asJava,
      schema))
  }
}
