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

import java.io.IOException
import java.util.Collections
import java.util.concurrent.{AbstractExecutorService, CountDownLatch, Executors, Future, RejectedExecutionException, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class PreparedIoBatchSuite extends AnyFunSuite {
  test("registration does not start I/O and launch is idempotent") {
    val executor = Executors.newFixedThreadPool(2)
    val taskRan = new AtomicBoolean(false)
    val readerOpened = new AtomicBoolean(false)
    val batch = new PreparedIoBatch(executor)
    val value = batch.registerTask(() => {
      taskRan.set(true)
      7
    })
    val readers = batch.registerIteratorGroup(Seq(preparer {
      readerOpened.set(true)
      iterator(1)
    }).asJava)

    try {
      assert(!taskRan.get())
      assert(!readerOpened.get())
      assertThrows[IllegalStateException](readers.get(0).hasNext)

      batch.launch()
      batch.launch()
      assert(value.get(5, TimeUnit.SECONDS) === 7)
      assert(readers.get(0).next() === 1)
      assert(taskRan.get())
      assert(readerOpened.get())
      assert(!executor.isShutdown)
    } finally {
      batch.close()
      shutdown(executor)
    }
  }

  test("launch supports an executor that runs submissions inline") {
    val executor = new DirectExecutorService
    val launcher = Executors.newSingleThreadExecutor()
    val taskRan = new AtomicBoolean(false)
    val readerOpened = new AtomicBoolean(false)
    val batch = new PreparedIoBatch(executor)
    batch.registerTask(() => taskRan.compareAndSet(false, true))
    val reader = batch.registerIteratorGroup(Seq(preparer {
      readerOpened.set(true)
      iterator(1)
    }).asJava).get(0)

    try {
      assert(!taskRan.get())
      assert(!readerOpened.get())
      launcher.submit(new Runnable {
        override def run(): Unit = batch.launch()
      }).get(5, TimeUnit.SECONDS)
      assert(taskRan.get())
      assert(readerOpened.get())
      assert(reader.next() === 1)
    } finally {
      batch.close()
      shutdown(launcher)
      shutdown(executor)
    }
  }

  test("close before launch cancels work without running it") {
    val executor = Executors.newSingleThreadExecutor()
    val taskRan = new AtomicBoolean(false)
    val readerOpened = new AtomicBoolean(false)
    val batch = new PreparedIoBatch(executor)
    val task = batch.registerTask(() => taskRan.compareAndSet(false, true))
    val reader = batch.registerIteratorGroup(Seq(preparer {
      readerOpened.set(true)
      iterator(1)
    }).asJava).get(0)
    val output = batch.own(reader)

    try {
      assertThrows[IllegalStateException](batch.own(reader))
      output.close()
      assert(task.isCancelled)
      assert(!taskRan.get())
      assert(!readerOpened.get())
      assertThrows[IllegalStateException](batch.launch())
      assert(!executor.isShutdown)
    } finally {
      output.close()
      shutdown(executor)
    }
  }

  test("submission rejection closes readers opened by earlier submissions") {
    val executor = new RejectingExecutorService(1)
    val firstClosed = new AtomicBoolean(false)
    val secondOpened = new AtomicBoolean(false)
    val batch = new PreparedIoBatch(executor)
    batch.registerIteratorGroup(Seq(
      preparer(iterator(1, firstClosed)),
      preparer {
        secondOpened.set(true)
        iterator(2)
      }).asJava)

    try {
      assertThrows[RejectedExecutionException](batch.launch())
      assert(firstClosed.get())
      assert(!secondOpened.get())
      assert(!executor.isShutdown)
    } finally {
      batch.close()
      shutdown(executor)
    }
  }

  test("launch interleaves source groups with each dependency before its reader") {
    val executor = Executors.newSingleThreadExecutor()
    val events = ArrayBuffer.empty[String]
    val batch = new PreparedIoBatch(executor)

    def dependency(name: String): Future[String] = batch.registerTask(() => {
      events.synchronized(events += name)
      name
    })

    def reader(name: String): PreparedIoBatch.IteratorPreparer[Integer] = preparer {
      events.synchronized(events += name)
      iterator(1)
    }

    val aDependencies = Seq(dependency("a0-task"), dependency("a1-task"))
    val bDependencies = Seq(dependency("b0-task"), dependency("b1-task"))
    val aReaders = batch.registerIteratorGroup(
      Seq(reader("a0-reader"), reader("a1-reader")).asJava,
      aDependencies.asJava)
    val bReaders = batch.registerIteratorGroup(
      Seq(reader("b0-reader"), reader("b1-reader")).asJava,
      bDependencies.asJava)

    try {
      batch.launch()
      (aReaders.asScala ++ bReaders.asScala).foreach(_.hasNext)
      assert(events.synchronized(events.toSeq) === Seq(
        "a0-task",
        "a0-reader",
        "b0-task",
        "b0-reader",
        "a1-task",
        "a1-reader",
        "b1-task",
        "b1-reader"))
    } finally {
      batch.close()
      shutdown(executor)
    }
  }

  test("closing owned output cancels a leading task before its reader opens") {
    val executor = Executors.newSingleThreadExecutor()
    val taskStarted = new CountDownLatch(1)
    val taskInterrupted = new CountDownLatch(1)
    val readerOpened = new AtomicBoolean(false)
    val batch = new PreparedIoBatch(executor)
    val leadingTask = batch.registerTask(() => {
      taskStarted.countDown()
      try {
        new CountDownLatch(1).await()
        1
      } catch {
        case failure: InterruptedException =>
          taskInterrupted.countDown()
          throw failure
      }
    })
    val readers = batch.registerIteratorGroup(
      Seq(preparer {
        readerOpened.set(true)
        iterator(1)
      }).asJava,
      Seq(leadingTask).asJava)
    val output = batch.own(readers.get(0))

    try {
      batch.launch()
      assert(taskStarted.await(5, TimeUnit.SECONDS))
      output.close()
      assert(taskInterrupted.await(5, TimeUnit.SECONDS))
      assert(leadingTask.isCancelled)
      assert(!readerOpened.get())
      assert(!executor.isShutdown)
    } finally {
      output.close()
      shutdown(executor)
    }
  }

  test("close closes every opened reader and preserves suppressed failures") {
    val executor = Executors.newFixedThreadPool(2)
    val opened = new CountDownLatch(2)
    val batch = new PreparedIoBatch(executor)
    batch.registerIteratorGroup(Seq(
      failingPreparer("first", opened),
      failingPreparer("second", opened)).asJava)

    try {
      batch.launch()
      assert(opened.await(5, TimeUnit.SECONDS))
      val failure = intercept[IOException](batch.close())
      assert(failure.getMessage === "first")
      assert(failure.getSuppressed.map(_.getMessage).toSeq === Seq("second"))
    } finally {
      batch.close()
      shutdown(executor)
    }
  }

  test("close restores the caller interrupt status after awaiting a worker") {
    val executor = Executors.newSingleThreadExecutor()
    val started = new CountDownLatch(1)
    val closed = new CountDownLatch(1)
    val batch = new PreparedIoBatch(executor)
    batch.registerIteratorGroup(Seq(preparer {
      new CloseableIterator[Integer] {
        override def hasNext: Boolean = true

        override def next(): Integer = {
          started.countDown()
          while (closed.getCount > 0) {
            try closed.await()
            catch { case _: InterruptedException => () }
          }
          val until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(25)
          while (System.nanoTime() < until) {}
          1
        }

        override def close(): Unit = closed.countDown()
      }
    }).asJava)

    try {
      batch.launch()
      assert(started.await(5, TimeUnit.SECONDS))
      Thread.currentThread().interrupt()
      batch.close()
      assert(Thread.currentThread().isInterrupted)
    } finally {
      Thread.interrupted()
      batch.close()
      shutdown(executor)
    }
  }

  private def preparer(
      open: => CloseableIterator[Integer]): PreparedIoBatch.IteratorPreparer[Integer] =
    () => open

  private def failingPreparer(
      message: String,
      opened: CountDownLatch): PreparedIoBatch.IteratorPreparer[Integer] = preparer {
    opened.countDown()
    new CloseableIterator[Integer] {
      private var available = true

      override def hasNext: Boolean = available

      override def next(): Integer = {
        available = false
        1
      }

      override def close(): Unit = throw new IOException(message)
    }
  }

  private def iterator(value: Integer): CloseableIterator[Integer] =
    iterator(value, new AtomicBoolean(false))

  private def iterator(
      value: Integer,
      closed: AtomicBoolean): CloseableIterator[Integer] =
    new CloseableIterator[Integer] {
      private var available = true

      override def hasNext: Boolean = available

      override def next(): Integer = {
        available = false
        value
      }

      override def close(): Unit = closed.set(true)
    }

  private def shutdown(executor: java.util.concurrent.ExecutorService): Unit = {
    executor.shutdownNow()
    assert(executor.awaitTermination(5, TimeUnit.SECONDS))
  }

  private class DirectExecutorService extends AbstractExecutorService {
    @volatile private var stopped = false

    override def execute(command: Runnable): Unit = command.run()

    override def shutdown(): Unit = stopped = true

    override def shutdownNow(): java.util.List[Runnable] = {
      stopped = true
      Collections.emptyList()
    }

    override def isShutdown: Boolean = stopped

    override def isTerminated: Boolean = stopped

    override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = stopped
  }

  private class RejectingExecutorService(acceptedSubmissions: Int)
      extends DirectExecutorService {
    private val submissions = new AtomicInteger()

    override def execute(command: Runnable): Unit = {
      if (submissions.getAndIncrement() >= acceptedSubmissions) {
        throw new RejectedExecutionException("rejected for test")
      }
      super.execute(command)
    }
  }
}
