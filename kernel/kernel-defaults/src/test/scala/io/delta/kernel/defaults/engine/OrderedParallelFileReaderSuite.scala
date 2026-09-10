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
package io.delta.kernel.defaults.engine

import java.io.{IOException, UncheckedIOException}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.scalatest.funsuite.AnyFunSuite

class OrderedParallelFileReaderSuite extends AnyFunSuite {
  private case class Batch(file: Int, value: Int)

  test("reads files concurrently and streams results in file order") {
    val started = new CountDownLatch(2)
    val release = new CountDownLatch(1)
    val closed = new AtomicInteger()
    val result = OrderedParallelFileReader.read(
      files(3),
      2,
      file => iterator(Seq(0, 1).map(Batch(file.getPath.toInt, _)), started, release, closed))

    assert(started.await(5, TimeUnit.SECONDS))
    release.countDown()
    try {
      assert(result.asScala.toSeq === Seq(
        Batch(0, 0),
        Batch(0, 1),
        Batch(1, 0),
        Batch(1, 1),
        Batch(2, 0),
        Batch(2, 1)))
      assert(closed.get() === 3)
    } finally {
      result.close()
    }
  }

  test("propagates reader failures") {
    val result = OrderedParallelFileReader.read(
      files(2),
      2,
      file => {
        if (file.getPath == "1") throw new IOException("failed")
        toCloseableIterator(Seq.empty[Batch].iterator.asJava)
      })

    val error = intercept[UncheckedIOException](result.hasNext)
    assert(error.getCause.getMessage === "failed")
  }

  test("close interrupts blocked producers and closes their readers") {
    val started = new CountDownLatch(2)
    val closed = new CountDownLatch(2)
    val result = OrderedParallelFileReader.read(
      files(2),
      2,
      file =>
        new CloseableIterator[Batch] {
          started.countDown()
          override def hasNext: Boolean = true
          override def next(): Batch = Batch(file.getPath.toInt, 0)
          override def close(): Unit = closed.countDown()
        })

    assert(started.await(5, TimeUnit.SECONDS))
    result.close()
    assert(closed.await(5, TimeUnit.SECONDS))
  }

  private def files(count: Int): CloseableIterator[FileStatus] =
    toCloseableIterator(
      (0 until count).map(index => FileStatus.of(index.toString, 0, 0)).iterator.asJava)

  private def iterator(
      values: Seq[Batch],
      started: CountDownLatch,
      release: CountDownLatch,
      closed: AtomicInteger): CloseableIterator[Batch] = new CloseableIterator[Batch] {
    private val delegate = values.iterator
    started.countDown()
    override def hasNext: Boolean = {
      release.await()
      delegate.hasNext
    }
    override def next(): Batch = delegate.next()
    override def close(): Unit = closed.incrementAndGet()
  }
}
