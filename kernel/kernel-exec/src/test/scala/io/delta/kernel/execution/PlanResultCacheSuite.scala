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

package io.delta.kernel.execution

import java.util.{Collections, Optional}
import java.util.concurrent.{Callable, CompletableFuture, Executors, TimeoutException, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.data.{ColumnarBatch, Row}
import io.delta.kernel.data.ColumnarBatch.Lifetime
import io.delta.kernel.internal.data.{GenericRow, RowBackedColumnarBatch}
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.plans.PlanNode.FileScan
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class PlanResultCacheSuite extends AnyFunSuite {
  private val schema = new StructType().add("id", IntegerType.INTEGER, false)

  test("a ready result is claimed once and first prefetch wins") {
    val cache = new PlanResultCache(4, Long.MaxValue)
    val first = new TrackingIterator(Seq(batch(1)))
    val second = new TrackingIterator(Seq(batch(2)))
    try {
      assert(cache.prefetch(scan(1), completed(first), 1))
      assert(!cache.prefetch(scan(1), completed(second), 1))
      assert(second.closed)
      assert(collectInts(cache.get(scan(1))) === Seq(1))
      assert(cache.get(scan(1)) == null)
    } finally {
      cache.close()
    }
  }

  test("a future result waits on get and is claimed once") {
    val cache = new PlanResultCache(4, Long.MaxValue)
    val pending = new CompletableFuture[CloseableIterator[ColumnarBatch]]()
    val worker = Executors.newSingleThreadExecutor()
    try {
      assert(cache.prefetch(scan(1), pending, 1))
      val get = worker.submit(new Callable[Int] {
        override def call(): Int = {
          val result = cache.get(scan(1))
          try {
            assert(result.hasNext)
            result.next().getColumnVector(0).getInt(0)
          } finally {
            result.close()
          }
        }
      })
      intercept[TimeoutException] {
        get.get(100, TimeUnit.MILLISECONDS)
      }
      pending.complete(Utils.singletonCloseableIterator(batch(7)))
      assert(get.get(5, TimeUnit.SECONDS) === 7)
      assert(cache.get(scan(1)) == null)
    } finally {
      worker.shutdownNow()
      cache.close()
    }
  }

  test("a failed future is removed and may be retried") {
    val cache = new PlanResultCache(4, Long.MaxValue)
    val failed = new CompletableFuture[CloseableIterator[ColumnarBatch]]()
    try {
      cache.prefetch(scan(1), failed, 1)
      failed.completeExceptionally(new IllegalStateException("boom"))
      assert(intercept[IllegalStateException](cache.get(scan(1))).getMessage === "boom")
      assert(cache.get(scan(1)) == null)

      cache.prefetch(scan(1), completed(new TrackingIterator(Seq(batch(3)))), 1)
      assert(collectInts(cache.get(scan(1))) === Seq(3))
    } finally {
      cache.close()
    }
  }

  test("entry eviction is FIFO and oversized results are rejected") {
    val cache = new PlanResultCache(1, Long.MaxValue)
    val first = new TrackingIterator(Seq(batch(1)))
    val second = new TrackingIterator(Seq(batch(2)))
    try {
      cache.prefetch(scan(1), completed(first), 1)
      cache.prefetch(scan(2), completed(second), 1)
      assert(first.closed)
      assert(cache.get(scan(1)) == null)
      assert(collectInts(cache.get(scan(2))) === Seq(2))
    } finally {
      cache.close()
    }

    val bounded = new PlanResultCache(4, 2)
    val retained = new TrackingIterator(Seq(batch(1)))
    val oversized = new TrackingIterator(Seq(batch(2)))
    try {
      assert(bounded.prefetch(scan(1), completed(retained), 1))
      assert(!bounded.prefetch(scan(2), completed(oversized), 3))
      assert(retained.closed)
      assert(oversized.closed)
      assert(bounded.get(scan(1)) == null)
      assert(bounded.get(scan(2)) == null)
    } finally {
      bounded.close()
    }
  }

  private def scan(id: Int): FileScan =
    FileScan.json(
      Seq.empty.asJava,
      Optional.empty(),
      Seq.empty.asJava,
      new StructType().add(s"id$id", IntegerType.INTEGER, false))

  private def row(value: Int): Row =
    new GenericRow(
      schema,
      Collections.singletonMap[Integer, Object](Int.box(0), Int.box(value)))

  private def batch(value: Int): ColumnarBatch =
    new RowBackedColumnarBatch(schema, Seq(row(value)).asJava, Lifetime.OWNED)

  private def completed(result: CloseableIterator[ColumnarBatch]) =
    CompletableFuture.completedFuture(result)

  private def collectInts(result: CloseableIterator[ColumnarBatch]): Seq[Int] = {
    val values = ArrayBuffer.empty[Int]
    try {
      while (result.hasNext) {
        val batch = result.next()
        var rowId = 0
        while (rowId < batch.getSize) {
          values += batch.getColumnVector(0).getInt(rowId)
          rowId += 1
        }
      }
    } finally {
      result.close()
    }
    values.toSeq
  }

  private final class TrackingIterator(batches: Seq[ColumnarBatch])
      extends CloseableIterator[ColumnarBatch] {
    private val iterator = batches.iterator
    var closed = false

    override def hasNext: Boolean = iterator.hasNext
    override def next(): ColumnarBatch = iterator.next()
    override def close(): Unit = closed = true
  }
}
