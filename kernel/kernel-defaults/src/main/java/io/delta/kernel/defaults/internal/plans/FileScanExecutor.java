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
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.SelectionColumnVector;
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.plans.FileScan;
import io.delta.kernel.internal.plans.FileType;
import io.delta.kernel.internal.plans.ScanFile;
import io.delta.kernel.internal.plans.ScanJson;
import io.delta.kernel.internal.plans.ScanParquet;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MetadataColumnSpec;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

/** Executes Parquet and JSON scan sources through the Kernel Java handlers. */
final class FileScanExecutor {
  private static final String PRIVATE_ROW_INDEX = "__delta_kernel_scan_row_index";
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

    return openPerFile(
        scan.getFiles(),
        file -> openParquet(scan, layout, Collections.singletonList(file), engine));
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
    return execute(
        FileType.PARQUET,
        scan.getSchema(),
        scan.getFileConstantColumns(),
        Optional.empty(),
        scan.getFiles(),
        engine,
        ioExecutor);
  }

  /** Registers a Parquet source in a query-owned batch without launching or owning that batch. */
  static CloseableIterator<FilteredColumnarBatch> prepare(
      ScanParquet scan, Engine engine, PreparedIoBatch ioBatch) {
    requireNonNull(scan, "scan is null");
    return prepare(
        FileType.PARQUET,
        scan.getSchema(),
        scan.getFileConstantColumns(),
        Optional.empty(),
        scan.getFiles(),
        requireNonNull(engine, "engine is null"),
        requireNonNull(ioBatch, "I/O batch is null"));
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

    return openPerFile(
        scan.getFiles(),
        file -> openJson(scan, layout, Collections.singletonList(file), file, engine));
  }

  /** JSON counterpart of {@link #execute(ScanParquet, Engine, ExecutorService)}. */
  static CloseableIterator<FilteredColumnarBatch> execute(
      ScanJson scan, Engine engine, ExecutorService ioExecutor) {
    requireNonNull(scan, "scan is null");
    requireNonNull(engine, "engine is null");
    requireNonNull(ioExecutor, "ioExecutor is null");
    return execute(
        FileType.JSON,
        scan.getSchema(),
        scan.getFileConstantColumns(),
        Optional.empty(),
        scan.getFiles(),
        engine,
        ioExecutor);
  }

  /** Registers a JSON source in a query-owned batch without launching or owning that batch. */
  static CloseableIterator<FilteredColumnarBatch> prepare(
      ScanJson scan, Engine engine, PreparedIoBatch ioBatch) {
    requireNonNull(scan, "scan is null");
    return prepare(
        FileType.JSON,
        scan.getSchema(),
        scan.getFileConstantColumns(),
        Optional.empty(),
        scan.getFiles(),
        requireNonNull(engine, "engine is null"),
        requireNonNull(ioBatch, "I/O batch is null"));
  }

  /** Executes scan files through one shared status, deletion-vector, and prepared-I/O path. */
  static CloseableIterator<FilteredColumnarBatch> execute(
      FileType fileType,
      StructType outputSchema,
      List<String> fileConstantColumns,
      Optional<URI> deletionVectorRoot,
      List<ScanFile> files,
      Engine engine,
      ExecutorService ioExecutor) {
    requireNonNull(fileType, "fileType is null");
    requireNonNull(outputSchema, "outputSchema is null");
    requireNonNull(fileConstantColumns, "fileConstantColumns is null");
    requireNonNull(deletionVectorRoot, "deletionVectorRoot is null");
    requireNonNull(files, "files is null");
    requireNonNull(engine, "engine is null");
    requireNonNull(ioExecutor, "ioExecutor is null");

    PreparedIoBatch ioBatch = new PreparedIoBatch(ioExecutor);
    try {
      CloseableIterator<FilteredColumnarBatch> prepared =
          prepare(
              fileType,
              outputSchema,
              fileConstantColumns,
              deletionVectorRoot,
              files,
              engine,
              ioBatch);
      ioBatch.launch();
      return ioBatch.own(prepared);
    } catch (RuntimeException | Error failure) {
      Utils.closeCloseablesAndAddSuppressed(failure, ioBatch);
      throw failure;
    }
  }

  /** Registers one ordered scan source without launching or claiming the supplied batch. */
  private static CloseableIterator<FilteredColumnarBatch> prepare(
      FileType fileType,
      StructType outputSchema,
      List<String> fileConstantColumns,
      Optional<URI> deletionVectorRoot,
      List<ScanFile> files,
      Engine engine,
      PreparedIoBatch ioBatch) {
    boolean hasDeletionVector =
        files.stream().anyMatch(file -> file.getDeletionVector().isPresent());
    ScanSchema scanSchema = scanSchema(outputSchema, hasDeletionVector);
    validate(fileType, scanSchema.schema, fileConstantColumns);
    validateDeletionVectors(files, deletionVectorRoot);

    List<Future<RoaringBitmapArray>> deletionVectors = new ArrayList<>(files.size());
    List<PreparedIoBatch.IteratorPreparer<FilteredColumnarBatch>> preparers =
        new ArrayList<>(files.size());
    for (ScanFile file : files) {
      deletionVectors.add(registerDeletionVector(file, deletionVectorRoot, engine, ioBatch));
      preparers.add(() -> openFile(fileType, scanSchema, fileConstantColumns, file, engine));
    }
    List<CloseableIterator<FilteredColumnarBatch>> preparedReaders =
        ioBatch.registerIteratorGroup(preparers, deletionVectors);
    List<CloseableIterator<FilteredColumnarBatch>> readers =
        new ArrayList<>(preparedReaders.size());
    for (int ordinal = 0; ordinal < preparedReaders.size(); ordinal++) {
      readers.add(
          applyDeletionVector(
              preparedReaders.get(ordinal), scanSchema, deletionVectors.get(ordinal)));
    }
    return combine(readers);
  }

  static void validate(FileType fileType, StructType schema, List<String> fileConstantColumns) {
    FileScan scan = newScan(fileType, Collections.emptyList(), fileConstantColumns, schema);
    if (scan instanceof ScanParquet) {
      validate((ScanParquet) scan);
    } else {
      validate((ScanJson) scan);
    }
  }

  static void validate(
      FileType fileType,
      StructType outputSchema,
      List<String> fileConstantColumns,
      boolean deletionVectors) {
    validate(fileType, scanSchema(outputSchema, deletionVectors).schema, fileConstantColumns);
  }

  static void validate(ScanParquet scan) {
    ScanLayout.forParquet(requireNonNull(scan, "scan is null"));
  }

  static void validate(ScanJson scan) {
    ScanLayout.forJson(requireNonNull(scan, "scan is null"));
  }

  private static CloseableIterator<FilteredColumnarBatch> openFile(
      FileType fileType,
      ScanSchema scanSchema,
      List<String> fileConstantColumns,
      ScanFile file,
      Engine engine) {
    Optional<FileStatus> knownStatus = file.getKnownFileStatus();
    FileStatus status =
        knownStatus.isPresent() ? knownStatus.get() : readFileStatus(file.getPath(), engine);
    ScanFile scanFile = new ScanFile(status, file.getFileConstants());
    FileScan scan =
        newScan(
            fileType, Collections.singletonList(scanFile), fileConstantColumns, scanSchema.schema);
    if (scan instanceof ScanParquet) {
      return execute((ScanParquet) scan, engine);
    }
    return execute((ScanJson) scan, engine);
  }

  private static FileStatus readFileStatus(String location, Engine engine) {
    try {
      FileStatus actual = engine.getFileSystemClient().getFileStatus(location);
      return FileStatus.of(location, actual.getSize(), actual.getModificationTime());
    } catch (IOException failure) {
      throw new UncheckedIOException("Failed to get scan file status for " + location, failure);
    }
  }

  private static Future<RoaringBitmapArray> registerDeletionVector(
      ScanFile file, Optional<URI> deletionVectorRoot, Engine engine, PreparedIoBatch ioBatch) {
    Optional<DeletionVectorDescriptor> descriptor = file.getDeletionVector();
    if (!descriptor.isPresent()) {
      return null;
    }
    String tableRoot = deletionVectorRoot.map(URI::toString).orElse("");
    return ioBatch.registerTask(
        () -> {
          Tuple2<DeletionVectorDescriptor, RoaringBitmapArray> loaded =
              DeletionVectorUtils.loadNewDvAndBitmap(engine, tableRoot, descriptor.get());
          return loaded._2;
        });
  }

  private static CloseableIterator<FilteredColumnarBatch> applyDeletionVector(
      CloseableIterator<FilteredColumnarBatch> reader,
      ScanSchema scanSchema,
      Future<RoaringBitmapArray> deletionVector) {
    if (deletionVector == null && !scanSchema.dropRowIndex) {
      return reader;
    }
    return new DeletionVectorIterator(reader, scanSchema, deletionVector);
  }

  private static void validateDeletionVectors(
      List<ScanFile> files, Optional<URI> deletionVectorRoot) {
    if (deletionVectorRoot.isPresent()) {
      return;
    }
    for (ScanFile file : files) {
      Optional<DeletionVectorDescriptor> descriptor = file.getDeletionVector();
      if (descriptor.isPresent()
          && DeletionVectorDescriptor.UUID_DV_MARKER.equals(descriptor.get().getStorageType())) {
        throw new IllegalArgumentException(
            "A deletion-vector root is required for a persisted-relative deletion vector");
      }
    }
  }

  private static ScanSchema scanSchema(StructType outputSchema, boolean needsRowIndex) {
    if (!needsRowIndex) {
      return new ScanSchema(outputSchema, -1, false);
    }
    int existing = outputSchema.indexOf(MetadataColumnSpec.ROW_INDEX);
    if (existing >= 0) {
      return new ScanSchema(outputSchema, existing, false);
    }
    String name = PRIVATE_ROW_INDEX;
    while (outputSchema.indexOf(name) >= 0) {
      name += "_";
    }
    StructType augmented =
        outputSchema.add(StructField.createMetadataColumn(name, MetadataColumnSpec.ROW_INDEX));
    return new ScanSchema(augmented, outputSchema.length(), true);
  }

  private static FileScan newScan(
      FileType fileType,
      List<ScanFile> files,
      List<String> fileConstantColumns,
      StructType schema) {
    if (fileType == FileType.PARQUET) {
      return new ScanParquet(files, fileConstantColumns, schema);
    }
    if (fileType == FileType.JSON) {
      return new ScanJson(files, fileConstantColumns, schema);
    }
    throw new IllegalArgumentException("Unsupported scan file type: " + fileType);
  }

  private static <T> T await(Future<T> future, String action) {
    try {
      return future.get();
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new KernelEngineException("await " + action, failure);
    } catch (ExecutionException failure) {
      Throwable cause = failure.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new KernelEngineException(action, cause);
    } catch (CancellationException failure) {
      throw new KernelEngineException("await cancelled " + action, failure);
    }
  }

  private static void cancel(Future<?> future) {
    if (future != null) {
      future.cancel(true);
    }
  }

  private static CloseableIterator<FilteredColumnarBatch> openParquet(
      ScanParquet scan, ScanLayout layout, List<ScanFile> files, Engine engine) {
    CloseableIterator<FileReadResult> reader =
        openReader(
            files,
            "Parquet",
            statuses ->
                engine
                    .getParquetHandler()
                    .readParquetFiles(statuses, layout.readSchema(), Optional.empty()));

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
    CloseableIterator<ColumnarBatch> reader =
        openReader(
            files,
            "JSON",
            statuses ->
                engine
                    .getJsonHandler()
                    .readJsonFiles(statuses, layout.readSchema(), Optional.empty()));

    long[] nextRowIndex = {0};
    return reader.map(
        batch -> {
          long rowIndex = nextRowIndex[0];
          nextRowIndex[0] = Math.addExact(rowIndex, batch.getSize());
          return filtered(splice(scan, layout, constantFile, batch, rowIndex));
        });
  }

  private static <T> CloseableIterator<T> openReader(
      List<ScanFile> files, String format, ReaderOpener<T> opener) {
    CloseableIterator<FileStatus> statuses = fileStatuses(files);
    try {
      return requireNonNull(opener.open(statuses), format + " reader is null");
    } catch (IOException failure) {
      Utils.closeCloseablesSilently(statuses);
      throw new UncheckedIOException("Failed to open " + format + " scan", failure);
    } catch (RuntimeException | Error failure) {
      Utils.closeCloseablesSilently(statuses);
      throw failure;
    }
  }

  private static <T> CloseableIterator<T> openPerFile(
      List<ScanFile> files, Function<ScanFile, CloseableIterator<T>> opener) {
    List<CloseableIterator<T>> readers = new ArrayList<>(files.size());
    try {
      for (ScanFile file : files) {
        readers.add(opener.apply(file));
      }
      return combine(readers);
    } catch (RuntimeException | Error failure) {
      Utils.closeCloseablesAndAddSuppressed(failure, readers.toArray(new AutoCloseable[0]));
      throw failure;
    }
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

  static <T> CloseableIterator<T> combine(List<CloseableIterator<T>> iterators) {
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

  private static <T> CloseableIterator<T> emptyIterator() {
    return toCloseableIterator(Collections.emptyIterator());
  }

  @FunctionalInterface
  private interface ReaderOpener<T> {
    CloseableIterator<T> open(CloseableIterator<FileStatus> statuses) throws IOException;
  }

  private static final class DeletionVectorIterator
      implements CloseableIterator<FilteredColumnarBatch> {
    private final CloseableIterator<FilteredColumnarBatch> delegate;
    private final ScanSchema scanSchema;
    private final Future<RoaringBitmapArray> deletionVector;
    private RoaringBitmapArray bitmap;
    private boolean loaded;
    private boolean closed;

    private DeletionVectorIterator(
        CloseableIterator<FilteredColumnarBatch> delegate,
        ScanSchema scanSchema,
        Future<RoaringBitmapArray> deletionVector) {
      this.delegate = requireNonNull(delegate, "scan file reader is null");
      this.scanSchema = scanSchema;
      this.deletionVector = deletionVector;
    }

    @Override
    public boolean hasNext() {
      try {
        boolean available = delegate.hasNext();
        if (!available) {
          cancel(deletionVector);
        }
        return available;
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(failure);
        throw failure;
      }
    }

    @Override
    public FilteredColumnarBatch next() {
      try {
        FilteredColumnarBatch batch = requireNonNull(delegate.next(), "scan reader batch is null");
        if (!loaded && deletionVector != null) {
          bitmap = await(deletionVector, "scan deletion vector");
          loaded = true;
        }
        if (bitmap == null) {
          if (!scanSchema.dropRowIndex) {
            return batch;
          }
          return new FilteredColumnarBatch(
              batch.getData().withDeletedColumnAt(scanSchema.rowIndexOrdinal),
              batch.getSelectionVector());
        }
        if (batch.getSelectionVector().isPresent()) {
          throw new IllegalStateException("A file scan unexpectedly returned selected rows");
        }

        ColumnarBatch data = batch.getData();
        ColumnVector rowIndices = data.getColumnVector(scanSchema.rowIndexOrdinal);
        SelectionColumnVector selection =
            scanSchema.dropRowIndex
                ? new SelectionColumnVector(bitmap, rowIndices)
                : SelectionColumnVector.borrowing(bitmap, rowIndices);
        ColumnarBatch output =
            scanSchema.dropRowIndex ? data.withDeletedColumnAt(scanSchema.rowIndexOrdinal) : data;
        return new FilteredColumnarBatch(output, Optional.of(selection));
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(failure);
        throw failure;
      }
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        cancel(deletionVector);
        delegate.close();
      }
    }

    private void closeAfterFailure(Throwable failure) {
      if (!closed) {
        closed = true;
        cancel(deletionVector);
        Utils.closeCloseablesAndAddSuppressed(failure, delegate);
      }
    }
  }

  private static final class ScanSchema {
    private final StructType schema;
    private final int rowIndexOrdinal;
    private final boolean dropRowIndex;

    private ScanSchema(StructType schema, int rowIndexOrdinal, boolean dropRowIndex) {
      this.schema = schema;
      this.rowIndexOrdinal = rowIndexOrdinal;
      this.dropRowIndex = dropRowIndex;
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
