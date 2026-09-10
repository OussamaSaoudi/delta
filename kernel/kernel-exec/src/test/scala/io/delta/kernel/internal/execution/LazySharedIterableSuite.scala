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

package io.delta.kernel.internal.execution

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.data.{FilteredColumnarBatch, Row}
import io.delta.kernel.data.FilteredColumnarBatch.Lifetime
import io.delta.kernel.internal.data.{GenericRow, RowBackedColumnarBatch}
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class LazySharedIterableSuite extends AnyFunSuite with VectorTestUtils {
  test("a borrowed child batch is retained once without changing its shape or metadata") {
    val schema = new StructType().add("id", IntegerType.INTEGER, false)
    val rows: Seq[Row] = Seq(1, 2).map { value =>
      GenericRow.fromOwnedValues(schema, Array[AnyRef](Int.box(value)))
    }
    val borrowed = new FilteredColumnarBatch(
      new RowBackedColumnarBatch(schema, rows.asJava),
      Optional.of(booleanVector(Seq[java.lang.Boolean](false, true))),
      "file:/table/part.parquet",
      1,
      Lifetime.BORROWED)
    val child = new TrackingIterator(borrowed)
    val shared = new LazySharedIterable(child, 2)
    val first = shared.iterator()
    val second = shared.iterator()

    val fromFirst = first.next()
    val fromSecond = second.next()
    assert(fromFirst eq fromSecond)
    assert(!(fromFirst eq borrowed))
    assert(fromFirst.getLifetime === Lifetime.OWNED)
    assert(fromFirst.getData.getSize === 2)
    assert((0 until 2).map(fromFirst.isSelected) === Seq(false, true))
    assert(fromFirst.getFilePath === Optional.of("file:/table/part.parquet"))
    assert(fromFirst.getPreComputedNumSelectedRows === Optional.of(1))
    assert(child.nextCalls === 1)

    first.close()
    assert(!child.closed)
    second.close()
    assert(child.closed)
  }

  private final class TrackingIterator(batch: FilteredColumnarBatch)
      extends CloseableIterator[FilteredColumnarBatch] {
    private var available = true
    var nextCalls = 0
    var closed = false

    override def hasNext: Boolean = available

    override def next(): FilteredColumnarBatch = {
      available = false
      nextCalls += 1
      batch
    }

    override def close(): Unit = closed = true
  }
}
