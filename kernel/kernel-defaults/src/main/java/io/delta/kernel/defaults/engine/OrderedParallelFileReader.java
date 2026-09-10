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
package io.delta.kernel.defaults.engine;

import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Reads files concurrently while exposing their batches in input-file order. */
final class OrderedParallelFileReader {
  private static final Object END_OF_FILE = new Object();
  private static final AtomicInteger NEXT_THREAD_ID = new AtomicInteger();

  private OrderedParallelFileReader() {}

  static <T> CloseableIterator<T> read(
      CloseableIterator<FileStatus> files, int parallelism, FileOpener<T> opener)
      throws IOException {
    List<FileTask<T>> tasks = new ArrayList<>();
    try {
      while (files.hasNext()) {
        tasks.add(new FileTask<>(files.next(), opener));
      }
    } finally {
      files.close();
    }
    if (tasks.isEmpty()) {
      return Utils.toCloseableIterator(Collections.<T>emptyList().iterator());
    }

    ExecutorService executor =
        Executors.newFixedThreadPool(
            Math.min(parallelism, tasks.size()),
            runnable -> {
              Thread thread =
                  new Thread(
                      runnable, "delta-kernel-file-reader-" + NEXT_THREAD_ID.incrementAndGet());
              thread.setDaemon(true);
              return thread;
            });
    try {
      for (FileTask<T> task : tasks) {
        task.future = executor.submit(task);
      }
    } catch (RuntimeException | Error failure) {
      tasks.forEach(FileTask::close);
      executor.shutdownNow();
      throw failure;
    }
    return new ResultIterator<>(tasks, executor);
  }

  @FunctionalInterface
  interface FileOpener<T> {
    CloseableIterator<T> open(FileStatus file) throws IOException;
  }

  private static final class FileTask<T> implements Runnable {
    private final FileStatus file;
    private final FileOpener<T> opener;
    private final ArrayBlockingQueue<Object> results = new ArrayBlockingQueue<>(2);
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Future<?> future;

    private FileTask(FileStatus file, FileOpener<T> opener) {
      this.file = file;
      this.opener = opener;
    }

    @Override
    public void run() {
      try (CloseableIterator<T> batches = opener.open(file)) {
        while (!closed.get() && batches.hasNext()) {
          T batch = batches.next();
          if (!offer(batch)) {
            return;
          }
        }
      } catch (Throwable failure) {
        offer(new Failure(failure));
        return;
      }
      offer(END_OF_FILE);
    }

    private boolean offer(Object value) {
      if (closed.get()) {
        return false;
      }
      try {
        results.put(value);
        if (closed.get()) {
          results.remove(value);
          return false;
        }
        return true;
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return false;
      }
    }

    private Object take() {
      try {
        return results.take();
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while reading files", interrupted);
      }
    }

    private void close() {
      closed.set(true);
      if (future != null) {
        future.cancel(true);
      }
      results.clear();
    }
  }

  private static final class ResultIterator<T> implements CloseableIterator<T> {
    private final List<FileTask<T>> tasks;
    private final ExecutorService executor;
    private int taskIndex;
    private T next;
    private boolean closed;

    private ResultIterator(List<FileTask<T>> tasks, ExecutorService executor) {
      this.tasks = tasks;
      this.executor = executor;
    }

    @Override
    public boolean hasNext() {
      while (!closed && next == null && taskIndex < tasks.size()) {
        final Object result;
        try {
          result = tasks.get(taskIndex).take();
        } catch (RuntimeException | Error failure) {
          close();
          throw failure;
        }
        if (result == END_OF_FILE) {
          taskIndex++;
        } else if (result instanceof Failure) {
          close();
          throw propagate(((Failure) result).failure);
        } else {
          @SuppressWarnings("unchecked")
          T value = (T) result;
          next = value;
        }
      }
      if (taskIndex == tasks.size() && next == null) {
        close();
      }
      return next != null;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      T result = next;
      next = null;
      return result;
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        next = null;
        tasks.forEach(FileTask::close);
        executor.shutdownNow();
      }
    }
  }

  private static RuntimeException propagate(Throwable failure) {
    if (failure instanceof RuntimeException) {
      return (RuntimeException) failure;
    }
    if (failure instanceof IOException) {
      return new UncheckedIOException((IOException) failure);
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    return new RuntimeException(failure);
  }

  private static final class Failure {
    private final Throwable failure;

    private Failure(Throwable failure) {
      this.failure = failure;
    }
  }
}
