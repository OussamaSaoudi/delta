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
package io.delta.kernel.defaults.internal.plans;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultLongVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultSubFieldVector;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.exceptions.KernelEngineException;
import io.delta.kernel.internal.plans.FileScan;
import io.delta.kernel.internal.plans.ScanFile;
import io.delta.kernel.internal.plans.ScanJson;
import io.delta.kernel.internal.plans.ScanParquet;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MetadataColumnSpec;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

/** Executes Parquet and JSON scan sources through the Kernel Java handlers. */
final class FileScanExecutor {
  private static final int READ_COLUMN = -1;
  private static final int JSON_ROW_INDEX = -2;

  private FileScanExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(ScanParquet scan, Engine engine) {
    requireNonNull(scan, "scan is null");
    requireNonNull(engine, "engine is null");
    ScanLayout layout = ScanLayout.forParquet(scan);
    if (scan.getFiles().isEmpty()) {
      return emptyIterator();
    }
    if (scan.getFileConstantColumns().isEmpty() || !hasRepeatedPath(scan.getFiles())) {
      return openParquet(scan, layout, scan.getFiles(), engine);
    }

    List<CloseableIterator<FilteredColumnarBatch>> readers = new ArrayList<>();
    try {
      for (ScanFile file : scan.getFiles()) {
        readers.add(openParquet(scan, layout, Collections.singletonList(file), engine));
      }
      return combine(readers);
    } catch (RuntimeException failure) {
      closeSilently(readers);
      throw failure;
    }
  }

  /**
   * Opens one reader per file and starts every reader's first blocking read on {@code ioExecutor}.
   * Results remain pull-based and are emitted in the scan's declared file order.
   *
   * <p>The caller owns {@code ioExecutor}. Closing the result cancels its outstanding reads and
   * closes every file reader, but does not shut down the executor.
   */
  static CloseableIterator<FilteredColumnarBatch> execute(
      ScanParquet scan, Engine engine, ExecutorService ioExecutor) {
    requireNonNull(scan, "scan is null");
    requireNonNull(engine, "engine is null");
    requireNonNull(ioExecutor, "ioExecutor is null");
    ScanLayout layout = ScanLayout.forParquet(scan);
    List<CloseableIterator<FilteredColumnarBatch>> readers = new ArrayList<>();
    try {
      for (ScanFile file : scan.getFiles()) {
        CloseableIterator<FilteredColumnarBatch> reader =
            openParquet(scan, layout, Collections.singletonList(file), engine);
        readers.add(PrimedIterator.submit(reader, ioExecutor));
      }
      return closeOnFailure(combine(readers));
    } catch (RuntimeException | Error failure) {
      closeAfterFailure(failure, readers);
      throw failure;
    }
  }

  static CloseableIterator<FilteredColumnarBatch> execute(ScanJson scan, Engine engine) {
    requireNonNull(scan, "scan is null");
    requireNonNull(engine, "engine is null");
    ScanLayout layout = ScanLayout.forJson(scan);
    if (scan.getFiles().isEmpty()) {
      return emptyIterator();
    }
    if (scan.getFileConstantColumns().isEmpty() && !layout.hasJsonRowIndex()) {
      return openJson(scan, layout, scan.getFiles(), null, engine);
    }

    List<CloseableIterator<FilteredColumnarBatch>> readers = new ArrayList<>();
    try {
      for (ScanFile file : scan.getFiles()) {
        readers.add(openJson(scan, layout, Collections.singletonList(file), file, engine));
      }
      return combine(readers);
    } catch (RuntimeException failure) {
      closeSilently(readers);
      throw failure;
    }
  }

  /** JSON counterpart of {@link #execute(ScanParquet, Engine, ExecutorService)}. */
  static CloseableIterator<FilteredColumnarBatch> execute(
      ScanJson scan, Engine engine, ExecutorService ioExecutor) {
    requireNonNull(scan, "scan is null");
    requireNonNull(engine, "engine is null");
    requireNonNull(ioExecutor, "ioExecutor is null");
    ScanLayout layout = ScanLayout.forJson(scan);
    List<CloseableIterator<FilteredColumnarBatch>> readers = new ArrayList<>();
    try {
      for (ScanFile file : scan.getFiles()) {
        CloseableIterator<FilteredColumnarBatch> reader =
            openJson(scan, layout, Collections.singletonList(file), file, engine);
        readers.add(PrimedIterator.submit(reader, ioExecutor));
      }
      return closeOnFailure(combine(readers));
    } catch (RuntimeException | Error failure) {
      closeAfterFailure(failure, readers);
      throw failure;
    }
  }

  private static CloseableIterator<FilteredColumnarBatch> openParquet(
      ScanParquet scan, ScanLayout layout, List<ScanFile> files, Engine engine) {
    CloseableIterator<FileStatus> statuses = fileStatuses(files);
    final CloseableIterator<FileReadResult> reader;
    try {
      reader =
          requireNonNull(
              engine
                  .getParquetHandler()
                  .readParquetFiles(statuses, layout.readSchema(), Optional.empty()),
              "Parquet reader is null");
    } catch (IOException failure) {
      Utils.closeCloseablesSilently(statuses);
      throw new UncheckedIOException("Failed to open Parquet scan", failure);
    } catch (RuntimeException failure) {
      Utils.closeCloseablesSilently(statuses);
      throw failure;
    }

    Map<String, ScanFile> filesByPath = new HashMap<>();
    files.forEach(file -> filesByPath.put(file.getFileStatus().getPath(), file));
    return reader.map(
        result -> {
          requireNonNull(result, "Parquet reader result is null");
          ScanFile file = filesByPath.get(result.getFilePath());
          if (file == null) {
            throw new IllegalArgumentException(
                "Parquet reader returned unrequested file `" + result.getFilePath() + "`");
          }
          return filtered(splice(scan, layout, file, result.getData(), 0));
        });
  }

  private static CloseableIterator<FilteredColumnarBatch> openJson(
      ScanJson scan,
      ScanLayout layout,
      List<ScanFile> files,
      ScanFile constantFile,
      Engine engine) {
    CloseableIterator<FileStatus> statuses = fileStatuses(files);
    final CloseableIterator<ColumnarBatch> reader;
    try {
      reader =
          requireNonNull(
              engine
                  .getJsonHandler()
                  .readJsonFiles(statuses, layout.readSchema(), Optional.empty()),
              "JSON reader is null");
    } catch (IOException failure) {
      Utils.closeCloseablesSilently(statuses);
      throw new UncheckedIOException("Failed to open JSON scan", failure);
    } catch (RuntimeException failure) {
      Utils.closeCloseablesSilently(statuses);
      throw failure;
    }

    long[] nextRowIndex = {0};
    return reader.map(
        batch -> {
          long rowIndex = nextRowIndex[0];
          nextRowIndex[0] = Math.addExact(rowIndex, batch.getSize());
          return filtered(splice(scan, layout, constantFile, batch, rowIndex));
        });
  }

  private static ColumnarBatch splice(
      FileScan scan, ScanLayout layout, ScanFile file, ColumnarBatch batch, long jsonRowIndex) {
    requireNonNull(batch, "reader batch is null");
    if (!layout.readSchema().equals(batch.getSchema())) {
      throw new IllegalArgumentException(
          "Scan reader returned schema " + batch.getSchema() + ", expected " + layout.readSchema());
    }
    if (layout.readSchema().equals(scan.getSchema())) {
      return batch;
    }

    ColumnarBatch output = batch;
    for (int ordinal = 0; ordinal < layout.sources().length; ordinal++) {
      int source = layout.sources()[ordinal];
      StructField field = scan.getSchema().at(ordinal);
      if (source >= 0) {
        ColumnVector constant =
            new DefaultSubFieldVector(
                batch.getSize(),
                field.getDataType(),
                source,
                ignored -> requireNonNull(file, "constant scan file is null").getFileConstants());
        output = output.withNewColumn(ordinal, field, constant);
      } else if (source == JSON_ROW_INDEX) {
        long[] values = new long[batch.getSize()];
        for (int row = 0; row < values.length; row++) {
          values[row] = Math.addExact(jsonRowIndex, row);
        }
        output =
            output.withNewColumn(
                ordinal,
                field,
                new DefaultLongVector(LongType.LONG, values.length, Optional.empty(), values));
      }
    }
    return output;
  }

  private static CloseableIterator<FileStatus> fileStatuses(List<ScanFile> files) {
    return toCloseableIterator(files.stream().map(ScanFile::getFileStatus).iterator());
  }

  private static boolean hasRepeatedPath(List<ScanFile> files) {
    Set<String> paths = new HashSet<>();
    return files.stream().anyMatch(file -> !paths.add(file.getFileStatus().getPath()));
  }

  private static FilteredColumnarBatch filtered(ColumnarBatch batch) {
    return new FilteredColumnarBatch(batch, Optional.empty());
  }

  private static <T> CloseableIterator<T> combine(List<CloseableIterator<T>> iterators) {
    return combine(iterators, 0, iterators.size());
  }

  private static <T> CloseableIterator<T> combine(
      List<CloseableIterator<T>> iterators, int start, int end) {
    if (start == end) {
      return emptyIterator();
    }
    if (start + 1 == end) {
      return iterators.get(start);
    }
    int middle = (start + end) >>> 1;
    return combine(iterators, start, middle).combine(combine(iterators, middle, end));
  }

  private static void closeSilently(List<? extends AutoCloseable> readers) {
    for (AutoCloseable reader : readers) {
      Utils.closeCloseablesSilently(reader);
    }
  }

  private static void closeAfterFailure(Throwable failure, List<? extends AutoCloseable> readers) {
    for (AutoCloseable reader : readers) {
      try {
        reader.close();
      } catch (Throwable closeFailure) {
        if (closeFailure != failure) {
          failure.addSuppressed(closeFailure);
        }
      }
    }
  }

  private static <T> CloseableIterator<T> closeOnFailure(CloseableIterator<T> delegate) {
    return new CloseableIterator<T>() {
      private boolean closed;

      @Override
      public boolean hasNext() {
        if (closed) {
          return false;
        }
        try {
          return delegate.hasNext();
        } catch (RuntimeException | Error failure) {
          closeAfterFailure(failure);
          throw failure;
        }
      }

      @Override
      public T next() {
        if (closed) {
          throw new java.util.NoSuchElementException();
        }
        try {
          return delegate.next();
        } catch (RuntimeException | Error failure) {
          closeAfterFailure(failure);
          throw failure;
        }
      }

      @Override
      public void close() throws IOException {
        if (!closed) {
          closed = true;
          delegate.close();
        }
      }

      private void closeAfterFailure(Throwable failure) {
        try {
          close();
        } catch (Throwable closeFailure) {
          if (closeFailure != failure) {
            failure.addSuppressed(closeFailure);
          }
        }
      }
    };
  }

  private static <T> CloseableIterator<T> emptyIterator() {
    return toCloseableIterator(Collections.emptyIterator());
  }

  /** Keeps one asynchronously read batch buffered ahead of the consumer. */
  private static final class PrimedIterator<T> implements CloseableIterator<T> {
    private final CloseableIterator<T> delegate;
    private final ExecutorService executor;
    private TrackedFutureTask<ReadResult<T>> readAhead;
    private ReadResult<T> buffered;
    private boolean exhausted;
    private boolean closed;

    private PrimedIterator(CloseableIterator<T> delegate, ExecutorService executor) {
      this.delegate = delegate;
      this.executor = executor;
    }

    static <T> PrimedIterator<T> submit(CloseableIterator<T> delegate, ExecutorService executor) {
      requireNonNull(delegate, "file reader is null");
      requireNonNull(executor, "ioExecutor is null");
      PrimedIterator<T> iterator = new PrimedIterator<>(delegate, executor);
      try {
        iterator.submitRead();
        return iterator;
      } catch (RuntimeException | Error failure) {
        iterator.closeAfterFailure(failure);
        throw failure;
      }
    }

    @Override
    public boolean hasNext() {
      ensureOpen();
      if (buffered == null && !exhausted) {
        buffered = awaitRead();
        readAhead = null;
        exhausted = !buffered.isAvailable();
      }
      return !exhausted;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new java.util.NoSuchElementException();
      }
      T value = buffered.getValue();
      buffered = null;
      submitRead();
      return value;
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        cancelAndAwaitRead();
        delegate.close();
      }
    }

    private void submitRead() {
      readAhead =
          new TrackedFutureTask<>(
              () -> {
                if (!delegate.hasNext()) {
                  return ReadResult.empty();
                }
                return ReadResult.available(
                    requireNonNull(delegate.next(), "reader batch is null"));
              });
      executor.execute(readAhead);
    }

    private ReadResult<T> awaitRead() {
      try {
        return readAhead.get();
      } catch (InterruptedException failure) {
        Thread.currentThread().interrupt();
        throw new KernelEngineException("await a plan file read", failure);
      } catch (ExecutionException failure) {
        Throwable cause = failure.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
          throw (Error) cause;
        }
        throw new KernelEngineException("prefetch a plan file", cause);
      } catch (CancellationException failure) {
        throw new KernelEngineException("await a cancelled plan file read", failure);
      }
    }

    private void cancelAndAwaitRead() {
      if (readAhead != null) {
        readAhead.cancelTracked();
        readAhead.awaitExit();
        readAhead = null;
      }
    }

    private void closeAfterFailure(Throwable failure) {
      try {
        close();
      } catch (Throwable closeFailure) {
        if (closeFailure != failure) {
          failure.addSuppressed(closeFailure);
        }
      }
    }

    private void ensureOpen() {
      if (closed) {
        throw new IllegalStateException("Plan file reader is closed");
      }
    }
  }

  private static final class ReadResult<T> {
    private final T value;

    private ReadResult(T value) {
      this.value = value;
    }

    private static <T> ReadResult<T> empty() {
      return new ReadResult<>(null);
    }

    private static <T> ReadResult<T> available(T value) {
      return new ReadResult<>(value);
    }

    private boolean isAvailable() {
      return value != null;
    }

    private T getValue() {
      return value;
    }
  }

  /** Future whose exit latch distinguishes queued cancellation from a running read unwinding. */
  private static final class TrackedFutureTask<T> extends FutureTask<T> {
    private final CountDownLatch exited = new CountDownLatch(1);
    private boolean entered;
    private boolean cancelledBeforeRun;

    private TrackedFutureTask(Callable<T> callable) {
      super(callable);
    }

    @Override
    public void run() {
      synchronized (this) {
        if (cancelledBeforeRun) {
          return;
        }
        entered = true;
      }
      try {
        super.run();
      } finally {
        exited.countDown();
      }
    }

    private void cancelTracked() {
      boolean queued;
      synchronized (this) {
        queued = !entered;
        if (queued) {
          cancelledBeforeRun = true;
        }
        cancel(true);
      }
      if (queued) {
        exited.countDown();
      }
    }

    private void awaitExit() {
      boolean interrupted = false;
      while (true) {
        try {
          exited.await();
          break;
        } catch (InterruptedException failure) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static final class ScanLayout {
    private final StructType readSchema;
    private final int[] sources;
    private final boolean hasJsonRowIndex;

    private ScanLayout(StructType readSchema, int[] sources, boolean hasJsonRowIndex) {
      this.readSchema = readSchema;
      this.sources = sources;
      this.hasJsonRowIndex = hasJsonRowIndex;
    }

    static ScanLayout forParquet(ScanParquet scan) {
      return create(scan, false);
    }

    static ScanLayout forJson(ScanJson scan) {
      return create(scan, true);
    }

    private static ScanLayout create(FileScan scan, boolean json) {
      Map<String, Integer> constants = new HashMap<>();
      for (int slot = 0; slot < scan.getFileConstantColumns().size(); slot++) {
        constants.put(scan.getFileConstantColumns().get(slot), slot);
      }

      List<StructField> readFields = new ArrayList<>();
      int[] sources = new int[scan.getSchema().length()];
      boolean hasJsonRowIndex = false;
      for (int ordinal = 0; ordinal < sources.length; ordinal++) {
        StructField field = scan.getSchema().at(ordinal);
        Integer constant = constants.get(field.getName());
        if (constant != null) {
          sources[ordinal] = constant;
        } else if (json && field.isMetadataColumn()) {
          if (field.getMetadataColumnSpec() != MetadataColumnSpec.ROW_INDEX
              || !LongType.LONG.equals(field.getDataType())
              || field.isNullable()) {
            throw new IllegalArgumentException(
                "Unsupported JSON scan metadata column `" + field.getName() + "`");
          }
          sources[ordinal] = JSON_ROW_INDEX;
          hasJsonRowIndex = true;
        } else {
          sources[ordinal] = READ_COLUMN;
          readFields.add(field);
        }
      }
      return new ScanLayout(new StructType(readFields), sources, hasJsonRowIndex);
    }

    StructType readSchema() {
      return readSchema;
    }

    int[] sources() {
      return sources;
    }

    boolean hasJsonRowIndex() {
      return hasJsonRowIndex;
    }
  }
}
