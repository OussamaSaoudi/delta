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
import java.util.{Collections, NoSuchElementException}
import java.util.concurrent.{AbstractExecutorService, ConcurrentLinkedQueue, CountDownLatch, Executors, Future, LinkedBlockingQueue, RejectedExecutionException, ThreadPoolExecutor, TimeUnit}
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
    assertThrows[IllegalArgumentException] {
      batch.registerIteratorGroup(Seq(reader("reused")).asJava, Seq(aDependencies.head).asJava)
    }

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

  test("logical readers use a bounded number of submissions and open readers") {
    val readerCount = 10000
    val window = 4
    val pausedExecutor = new PausedExecutorService
    val pausedBatch = new PreparedIoBatch(pausedExecutor, window)
    pausedBatch.registerIteratorGroup(
      Seq.fill(readerCount)(preparer(iterator(1))).asJava)

    try {
      pausedBatch.launch()
      assert(pausedExecutor.queued === window)
    } finally {
      pausedBatch.close()
      shutdown(pausedExecutor)
    }

    val executor = Executors.newFixedThreadPool(window)
    val opened = new AtomicInteger()
    val maxOpened = new AtomicInteger()
    val allOpened = new CountDownLatch(window)
    val batch = new PreparedIoBatch(executor, window)
    batch.registerIteratorGroup(Seq.fill(readerCount)(preparer {
      val current = opened.incrementAndGet()
      maxOpened.accumulateAndGet(current, Math.max)
      allOpened.countDown()
      new CloseableIterator[Integer] {
        private var available = true

        override def hasNext: Boolean = available

        override def next(): Integer = {
          available = false
          1
        }

        override def close(): Unit = opened.decrementAndGet()
      }
    }).asJava)

    try {
      batch.launch()
      assert(allOpened.await(5, TimeUnit.SECONDS))
      assert(maxOpened.get() === window)
      assert(opened.get() === window)
    } finally {
      batch.close()
      assert(opened.get() === 0)
      shutdown(executor)
    }
  }

  test("default window follows fixed-pool capacity with a bounded opaque fallback") {
    val fixedExecutor = Executors.newFixedThreadPool(3)
    val fixedOpened = new AtomicInteger()
    val threeOpened = new CountDownLatch(3)
    val fixedBatch = new PreparedIoBatch(fixedExecutor)
    fixedBatch.registerIteratorGroup(Seq.fill(10)(preparer {
      fixedOpened.incrementAndGet()
      threeOpened.countDown()
      iterator(1)
    }).asJava)

    try {
      fixedBatch.launch()
      assert(threeOpened.await(5, TimeUnit.SECONDS))
      assert(fixedOpened.get() === 3)
    } finally {
      fixedBatch.close()
      shutdown(fixedExecutor)
    }

    val opaqueExecutor = new DirectExecutorService
    val opaqueOpened = new AtomicInteger()
    val opaqueBatch = new PreparedIoBatch(opaqueExecutor)
    opaqueBatch.registerIteratorGroup(Seq.fill(40)(preparer {
      opaqueOpened.incrementAndGet()
      iterator(1)
    }).asJava)

    try {
      opaqueBatch.launch()
      assert(opaqueOpened.get() === 32)
    } finally {
      opaqueBatch.close()
      shutdown(opaqueExecutor)
    }

    val wideExecutor = new FixedCapacityPausedExecutor(64)
    val wideBatch = new PreparedIoBatch(wideExecutor)
    wideBatch.registerIteratorGroup(Seq.fill(40)(preparer(iterator(1))).asJava)

    try {
      wideBatch.launch()
      assert(wideExecutor.queued === 40)
    } finally {
      wideBatch.close()
      shutdown(wideExecutor)
    }
  }

  test("initial reader window is round-robin across source groups") {
    withBatch(new DirectExecutorService, 2) { batch =>
      val events = ArrayBuffer.empty[String]
      def named(name: String): PreparedIoBatch.IteratorPreparer[Integer] = preparer {
        events += name
        iterator(1)
      }
      batch.registerIteratorGroup(Seq(named("a0"), named("a1")).asJava)
      batch.registerIteratorGroup(Seq(named("b0"), named("b1")).asJava)
      batch.launch()
      assert(events.toSeq === Seq("a0", "b0"))
    }
  }

  test("source lane advances when another source holds the only configured slot") {
    val executor = new DirectExecutorService
    val opened = ArrayBuffer.empty[String]
    val batch = new PreparedIoBatch(executor, 1)

    def named(name: String, value: Int): PreparedIoBatch.IteratorPreparer[Integer] = preparer {
      opened += name
      iterator(value)
    }

    val a = batch.registerIteratorGroup(Seq(named("a0", 0), named("a1", 1)).asJava)
    val b = batch.registerIteratorGroup(Seq(named("b0", 2), named("b1", 3)).asJava)
    val output = batch.own(FileScanExecutor.combine(a).combine(FileScanExecutor.combine(b)))

    try {
      batch.launch()
      assert(opened.toSeq === Seq("a0", "b0"))
      assert(output.toInMemoryList().asScala.map(_.intValue()).toSeq === Seq(0, 1, 2, 3))
      assert(opened.toSeq === Seq("a0", "b0", "a1", "b1"))
    } finally {
      output.close()
      shutdown(executor)
    }
  }

  test("every source gets a progress lane when source count exceeds the window") {
    val executor = new DirectExecutorService
    val opened = ArrayBuffer.empty[String]
    val batch = new PreparedIoBatch(executor, 2)
    val groups = (0 until 5).map { group =>
      batch.registerIteratorGroup((0 until 2).map { ordinal =>
        preparer {
          opened += s"$group-$ordinal"
          iterator(group * 10 + ordinal)
        }
      }.asJava)
    }
    val output = batch.own(FileScanExecutor.combine(groups.flatMap(_.asScala).asJava))

    try {
      batch.launch()
      assert(opened.toSeq === (0 until 5).map(group => s"$group-0"))
      assert(output.toInMemoryList().asScala.map(_.intValue()).toSeq ===
        Seq(0, 1, 10, 11, 20, 21, 30, 31, 40, 41))
    } finally {
      output.close()
      shutdown(executor)
    }
  }

  test("reverse read completion still emits declared file order") {
    val executor = Executors.newFixedThreadPool(3)
    val started = new CountDownLatch(3)
    val releases = Seq.fill(3)(new CountDownLatch(1))
    val finished = Seq.fill(3)(new CountDownLatch(1))
    val batch = new PreparedIoBatch(executor, 3)
    val readers = batch.registerIteratorGroup((0 until 3).map { value =>
      preparer(blockingIterator(value, started, releases(value), finished(value)))
    }.asJava)
    val output = batch.own(FileScanExecutor.combine(readers))

    try {
      batch.launch()
      assert(started.await(5, TimeUnit.SECONDS))
      Seq(2, 1, 0).foreach { ordinal =>
        releases(ordinal).countDown()
        assert(finished(ordinal).await(5, TimeUnit.SECONDS))
      }
      assert(output.toInMemoryList().asScala.map(_.intValue()).toSeq === Seq(0, 1, 2))
    } finally {
      releases.foreach(_.countDown())
      output.close()
      shutdown(executor)
    }
  }

  test("active reader keeps exactly one batch read ahead") {
    val executor = Executors.newSingleThreadExecutor()
    val firstRead = new CountDownLatch(1)
    val secondRead = new CountDownLatch(1)
    val reads = new AtomicInteger()
    val batch = new PreparedIoBatch(executor, 1)
    val output = batch.own(batch.registerIteratorGroup(Seq(preparer {
      val values = Iterator(1, 2, 3)
      new CloseableIterator[Integer] {
        override def hasNext: Boolean = values.hasNext

        override def next(): Integer = {
          val count = reads.incrementAndGet()
          if (count == 1) firstRead.countDown()
          if (count == 2) secondRead.countDown()
          values.next()
        }

        override def close(): Unit = ()
      }
    }).asJava).get(0))

    try {
      batch.launch()
      assert(firstRead.await(5, TimeUnit.SECONDS))
      assert(reads.get() === 1)
      assert(output.next() === 1)
      assert(secondRead.await(5, TimeUnit.SECONDS))
      Thread.sleep(25)
      assert(reads.get() === 2)
      assert(output.toInMemoryList().asScala.map(_.intValue()).toSeq === Seq(2, 3))
    } finally {
      output.close()
      shutdown(executor)
    }
  }

  test("empty readers refill the window iteratively") {
    val readerCount = 10000
    withBatch(new DirectExecutorService, 1) { batch =>
      val opened = new AtomicInteger()
      val readers = batch.registerIteratorGroup(Seq.fill(readerCount)(preparer {
        opened.incrementAndGet()
        emptyIterator()
      }).asJava)
      val output = batch.own(FileScanExecutor.combine(readers))
      batch.launch()
      assert(!output.hasNext)
      assert(opened.get() === readerCount)
    }
  }

  test("early close refills a slot and failure cancels pending work") {
    val executor = new DirectExecutorService
    val opened = new AtomicInteger()
    val batch = new PreparedIoBatch(executor, 1)
    val readers = batch.registerIteratorGroup(Seq(
      preparer { opened.incrementAndGet(); iterator(1) },
      preparer { opened.incrementAndGet(); iterator(2) },
      preparer { opened.incrementAndGet(); iterator(3) }).asJava)

    try {
      batch.launch()
      assert(opened.get() === 1)
      readers.get(1).close()
      assert(opened.get() === 1)
      readers.get(0).close()
      assert(opened.get() === 2)
      assert(readers.get(2).next() === 3)
    } finally {
      batch.close()
      shutdown(executor)
    }

    val failureExecutor = new DirectExecutorService
    val pendingTaskRan = new AtomicBoolean(false)
    val pendingReaderOpened = new AtomicBoolean(false)
    val failureBatch = new PreparedIoBatch(failureExecutor, 1)
    val pendingTask = failureBatch.registerTask(() => pendingTaskRan.compareAndSet(false, true))
    val failureReaders = failureBatch.registerIteratorGroup(
      Seq(
        preparer(new FailingIterator),
        preparer { pendingReaderOpened.set(true); iterator(2) }).asJava,
      Seq(null, pendingTask).asJava)

    try {
      failureBatch.launch()
      assertThrows[IllegalStateException](failureReaders.get(0).hasNext)
      assert(pendingTask.isCancelled)
      assert(!pendingTaskRan.get())
      assert(!pendingReaderOpened.get())
    } finally {
      failureBatch.close()
      shutdown(failureExecutor)
    }
  }

  test("exhausted reader close failure terminates the whole batch") {
    withBatch(new DirectExecutorService, 1) { batch =>
      val pendingOpened = new AtomicBoolean(false)
      val readers = batch.registerIteratorGroup(Seq(
        preparer {
          new CloseableIterator[Integer] {
            override def hasNext: Boolean = false
            override def next(): Integer = throw new NoSuchElementException()
            override def close(): Unit = throw new IOException("close failed")
          }
        },
        preparer { pendingOpened.set(true); iterator(2) }).asJava)
      batch.launch()
      val failure = intercept[io.delta.kernel.exceptions.KernelEngineException] {
        readers.get(0).hasNext
      }
      assert(failure.getCause.getMessage === "close failed")
      assert(!pendingOpened.get())
      assertThrows[IllegalStateException](batch.launch())
    }
  }

  test("read-ahead submission rejection closes the whole batch") {
    val executor = new RejectingExecutorService(1)
    val firstClosed = new AtomicBoolean(false)
    val pendingOpened = new AtomicBoolean(false)
    val batch = new PreparedIoBatch(executor, 1)
    val readers = batch.registerIteratorGroup(Seq(
      preparer(iterator(1, firstClosed)),
      preparer { pendingOpened.set(true); iterator(2) }).asJava)
    val output = batch.own(FileScanExecutor.combine(readers))

    try {
      batch.launch()
      assertThrows[RejectedExecutionException](output.next())
      assert(firstClosed.get())
      assert(!pendingOpened.get())
      assertThrows[IllegalStateException](batch.launch())
    } finally {
      output.close()
      shutdown(executor)
    }
  }

  test("close cancels and awaits a dynamically submitted read-ahead task") {
    val executor = Executors.newSingleThreadExecutor()
    val secondReadStarted = new CountDownLatch(1)
    val secondReadInterrupted = new CountDownLatch(1)
    val readerClosed = new CountDownLatch(1)
    val reads = new AtomicInteger()
    val batch = new PreparedIoBatch(executor, 1)
    val output = batch.own(batch.registerIteratorGroup(Seq(preparer {
      new CloseableIterator[Integer] {
        override def hasNext: Boolean = true

        override def next(): Integer = {
          if (reads.incrementAndGet() == 1) {
            1
          } else {
            secondReadStarted.countDown()
            try {
              new CountDownLatch(1).await()
              2
            } catch {
              case failure: InterruptedException =>
                secondReadInterrupted.countDown()
                throw failure
            }
          }
        }

        override def close(): Unit = readerClosed.countDown()
      }
    }).asJava).get(0))

    try {
      batch.launch()
      assert(output.next() === 1)
      assert(secondReadStarted.await(5, TimeUnit.SECONDS))
      output.close()
      assert(secondReadInterrupted.await(5, TimeUnit.SECONDS))
      assert(readerClosed.getCount === 0)
    } finally {
      output.close()
      shutdown(executor)
    }
  }

  private def preparer(
      open: => CloseableIterator[Integer]): PreparedIoBatch.IteratorPreparer[Integer] =
    () => open

  private def withBatch[T](
      executor: java.util.concurrent.ExecutorService,
      readerWindow: Int)(test: PreparedIoBatch => T): T =
    withBatch(new PreparedIoBatch(executor, readerWindow), executor)(test)

  private def withBatch[T](
      batch: PreparedIoBatch,
      executor: java.util.concurrent.ExecutorService)(test: PreparedIoBatch => T): T =
    try test(batch)
    finally {
      batch.close()
      shutdown(executor)
    }

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

  private def blockingIterator(
      value: Integer,
      started: CountDownLatch,
      release: CountDownLatch,
      finished: CountDownLatch): CloseableIterator[Integer] =
    new CloseableIterator[Integer] {
      private var available = true

      override def hasNext: Boolean = available

      override def next(): Integer = {
        started.countDown()
        release.await()
        available = false
        finished.countDown()
        value
      }

      override def close(): Unit = release.countDown()
    }

  private def emptyIterator(): CloseableIterator[Integer] =
    new CloseableIterator[Integer] {
      override def hasNext: Boolean = false

      override def next(): Integer = throw new NoSuchElementException()

      override def close(): Unit = ()
    }

  private class FailingIterator extends CloseableIterator[Integer] {
    override def hasNext: Boolean = true

    override def next(): Integer = throw new IllegalStateException("read failed")

    override def close(): Unit = ()
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

  private class PausedExecutorService extends DirectExecutorService {
    private val commands = new ConcurrentLinkedQueue[Runnable]()

    def queued: Int = commands.size()

    override def execute(command: Runnable): Unit = commands.add(command)
  }

  private class FixedCapacityPausedExecutor(capacity: Int)
      extends ThreadPoolExecutor(
        capacity,
        capacity,
        0,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue[Runnable]()) {
    private val commands = new ConcurrentLinkedQueue[Runnable]()

    def queued: Int = commands.size()

    override def execute(command: Runnable): Unit = commands.add(command)
  }
}
