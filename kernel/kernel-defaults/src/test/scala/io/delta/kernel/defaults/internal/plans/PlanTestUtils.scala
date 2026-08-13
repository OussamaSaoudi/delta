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

import java.util.Optional

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ColumnarBatch, ColumnVector, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.Assertions.assert

private[plans] object PlanTestUtils {
  def row(schema: StructType, values: AnyRef*): Row =
    GenericRow.fromValues(schema, values.asJava)

  def columnarBatch(schema: StructType, rows: Seq[Row]): ColumnarBatch =
    new DefaultRowBasedColumnarBatch(schema, rows.asJava)

  def batch(
      schema: StructType,
      rows: Seq[Row],
      selected: Option[Seq[java.lang.Boolean]] = None): FilteredColumnarBatch =
    new FilteredColumnarBatch(
      columnarBatch(schema, rows),
      Optional.ofNullable(selected.map(values => booleanVector(values: _*)).orNull))

  def selectedBatch(
      data: ColumnarBatch,
      selected: java.lang.Boolean*): FilteredColumnarBatch =
    new FilteredColumnarBatch(data, Optional.of(booleanVector(selected: _*)))

  def selectedBatch(
      data: ColumnarBatch,
      selected: Seq[java.lang.Boolean],
      filePath: String,
      preComputedNumSelectedRows: Int): FilteredColumnarBatch =
    new FilteredColumnarBatch(
      data,
      Optional.of(booleanVector(selected: _*)),
      filePath,
      preComputedNumSelectedRows)

  def booleanVector(values: java.lang.Boolean*): ColumnVector = {
    val nulls = values.map(_ == null).toArray
    val booleans = values.map(value => value != null && value.booleanValue()).toArray
    new DefaultBooleanVector(values.size, Optional.of(nulls), booleans)
  }

  def rows(batch: FilteredColumnarBatch): Seq[Row] = {
    val iterator = batch.getRows
    try iterator.asScala.toSeq
    finally iterator.close()
  }

  def rows(batches: Iterable[FilteredColumnarBatch]): Seq[Row] =
    batches.toSeq.flatMap(batch => rows(batch))

  def rows(batches: CloseableIterator[FilteredColumnarBatch]): Seq[Row] = {
    rows(collectBatches(batches))
  }

  def logicalRows(batch: FilteredColumnarBatch): Seq[Seq[Any]] =
    rows(batch).map(logicalRow)

  def assertRows(
      batches: CloseableIterator[FilteredColumnarBatch],
      expected: Seq[Row]): Unit = {
    val actual = rows(batches)
    assert(actual.map(_.getSchema) == expected.map(_.getSchema))
    assert(actual.map(logicalRow) == expected.map(logicalRow))
  }

  def assertBatches(
      batches: CloseableIterator[FilteredColumnarBatch],
      expected: Seq[FilteredColumnarBatch]): Unit = {
    val actual = collectBatches(batches)
    assert(actual.map(_.getData.getSchema) == expected.map(_.getData.getSchema))
    assert(actual.map(logicalRows) == expected.map(logicalRows))
  }

  private def collectBatches(
      batches: CloseableIterator[FilteredColumnarBatch]): Seq[FilteredColumnarBatch] = {
    val result = ArrayBuffer.empty[FilteredColumnarBatch]
    try {
      while (batches.hasNext) {
        result += batches.next()
      }
      result.toSeq
    } finally batches.close()
  }

  private def logicalRow(row: Row): Seq[Any] = {
    val data = columnarBatch(row.getSchema, Seq(row))
    row.getSchema.fields().asScala.zipWithIndex.map { case (field, ordinal) =>
      PlanValueUtils.canonicalize(data.getColumnVector(ordinal), field.getDataType, 0)
    }.toSeq
  }
}

private[plans] class TrackingIterator[T](
    values: Seq[T],
    hasNextFailure: Option[RuntimeException] = None,
    closeFailure: Option[RuntimeException] = None)
    extends CloseableIterator[T] {
  private var index = 0
  var hasNextCalls = 0
  var nextCalls = 0
  var closeCalls = 0

  override def hasNext: Boolean = {
    hasNextCalls += 1
    hasNextFailure.foreach(throw _)
    index < values.size
  }

  override def next(): T = {
    nextCalls += 1
    if (index >= values.size) throw new NoSuchElementException
    val result = values(index)
    index += 1
    result
  }

  override def close(): Unit = {
    closeCalls += 1
    closeFailure.foreach(throw _)
  }
}
