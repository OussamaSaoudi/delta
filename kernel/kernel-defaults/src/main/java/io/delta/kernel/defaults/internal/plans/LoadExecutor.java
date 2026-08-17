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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelEngineException;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.ColumnarBatchRow;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.data.SelectionColumnVector;
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.plans.FileScan;
import io.delta.kernel.internal.plans.FileType;
import io.delta.kernel.internal.plans.Load;
import io.delta.kernel.internal.plans.ScanFile;
import io.delta.kernel.internal.plans.ScanJson;
import io.delta.kernel.internal.plans.ScanParquet;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MetadataColumnSpec;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/** Executes a {@link Load} using Kernel Java file readers and deletion-vector utilities. */
final class LoadExecutor {
  private static final String PRIVATE_ROW_INDEX = "__delta_kernel_load_row_index";

  private LoadExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(
      Load load,
      StructType inputSchema,
      CloseableIterator<FilteredColumnarBatch> input,
      Engine engine,
      ExecutorService ioExecutor) {
    requireNonNull(load, "load is null");
    requireNonNull(inputSchema, "inputSchema is null");
    requireNonNull(input, "input is null");
    requireNonNull(engine, "engine is null");
    requireNonNull(ioExecutor, "ioExecutor is null");

    load.getOutputSchema(Collections.singletonList(inputSchema));
    BoundLoad bound = BoundLoad.bind(load, inputSchema);
    validateScan(load, load.getSchema());
    validateScan(load, scanSchema(load.getSchema(), true).schema);

    List<PreparedFile> files = materializeFiles(load, bound, input, engine, ioExecutor);
    List<CloseableIterator<FilteredColumnarBatch>> readers = new ArrayList<>(files.size());
    List<Future<RoaringBitmapArray>> deletionVectors = new ArrayList<>(files.size());
    try {
      for (PreparedFile file : files) {
        readers.add(openScan(load, file, engine, ioExecutor));
      }
      for (PreparedFile file : files) {
        deletionVectors.add(submitDeletionVector(load, file, engine, ioExecutor));
      }
      for (int index = 0; index < files.size(); index++) {
        RoaringBitmapArray deletionVector =
            await(deletionVectors.get(index), "Load deletion vector");
        if (deletionVector != null) {
          readers.set(
              index, applyDeletionVector(files.get(index), readers.get(index), deletionVector));
        }
      }
      return FileScanExecutor.combine(readers);
    } catch (RuntimeException | Error failure) {
      deletionVectors.forEach(future -> future.cancel(true));
      Utils.closeCloseablesSilently(readers.toArray(new AutoCloseable[0]));
      throw failure;
    }
  }

  private static List<PreparedFile> materializeFiles(
      Load load,
      BoundLoad bound,
      CloseableIterator<FilteredColumnarBatch> input,
      Engine engine,
      ExecutorService ioExecutor) {
    List<MaterializedFile> files = new ArrayList<>();
    try {
      try (CloseableIterator<FilteredColumnarBatch> batches = input) {
        while (batches.hasNext()) {
          FilteredColumnarBatch batch = requireNonNull(batches.next(), "Load input batch is null");
          ColumnarBatch data = batch.getData();
          checkArgument(
              bound.inputSchema.equals(data.getSchema()),
              "Load input batch schema %s does not match expected schema %s",
              data.getSchema(),
              bound.inputSchema);
          materializeBatch(load, bound, batch, engine, ioExecutor, files);
        }
      } catch (IOException failure) {
        throw new UncheckedIOException("Failed to close Load input", failure);
      }

      List<PreparedFile> prepared = new ArrayList<>(files.size());
      for (MaterializedFile file : files) {
        ScanSchema schema = scanSchema(load.getSchema(), file.deletionVector != null);
        ScanFile scanFile = new ScanFile(await(file.status, "Load file status"), file.constants);
        FileScan scan = newScan(load, scanFile, schema.schema);
        prepared.add(new PreparedFile(scan, file.deletionVector, schema));
      }
      return prepared;
    } catch (RuntimeException | Error failure) {
      files.forEach(file -> file.status.cancel(true));
      throw failure;
    }
  }

  private static void materializeBatch(
      Load load,
      BoundLoad bound,
      FilteredColumnarBatch batch,
      Engine engine,
      ExecutorService ioExecutor,
      List<MaterializedFile> files) {
    ColumnarBatch data = batch.getData();
    ColumnVector paths = null;
    ColumnVector sizes = null;
    ColumnVector deletionVectors = null;
    try {
      paths = bound.path.eval(data);
      sizes = bound.size.eval(data);
      deletionVectors = bound.deletionVector.eval(data);
      for (int rowId = 0; rowId < data.getSize(); rowId++) {
        if (!batch.isSelected(rowId)) {
          continue;
        }
        if (paths.isNullAt(rowId)) {
          throw new IllegalArgumentException("Load path must not be null");
        }
        String location = resolvePath(load.getBaseUri(), paths.getString(rowId));
        Row constants = bound.materializeConstants(data, rowId);
        DeletionVectorDescriptor dv =
            DeletionVectorDescriptor.fromColumnVector(deletionVectors, rowId);
        if (dv != null
            && DeletionVectorDescriptor.UUID_DV_MARKER.equals(dv.getStorageType())
            && !load.getBaseUri().isPresent()) {
          throw new IllegalArgumentException(
              "Load base URI is required for a persisted-relative deletion vector");
        }
        Future<FileStatus> status = fileStatus(location, sizes, rowId, engine, ioExecutor);
        files.add(new MaterializedFile(status, constants, dv));
      }
    } finally {
      Utils.closeCloseables(paths, sizes, deletionVectors);
    }
  }

  private static Future<FileStatus> fileStatus(
      String location, ColumnVector sizes, int rowId, Engine engine, ExecutorService ioExecutor) {
    if (!sizes.isNullAt(rowId)) {
      long size = sizes.getLong(rowId);
      if (size < 0) {
        throw new IllegalArgumentException("Load file size must be non-negative");
      }
      return CompletableFuture.completedFuture(FileStatus.of(location, size, 0));
    }
    return ioExecutor.submit(() -> readFileStatus(location, engine));
  }

  private static FileStatus readFileStatus(String location, Engine engine) {
    try {
      FileStatus actual = engine.getFileSystemClient().getFileStatus(location);
      return FileStatus.of(location, actual.getSize(), actual.getModificationTime());
    } catch (IOException failure) {
      throw new UncheckedIOException("Failed to get status for Load file " + location, failure);
    }
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

  private static String resolvePath(Optional<URI> baseUri, String path) {
    requireNonNull(path, "Load path is null");
    final URI parsed;
    try {
      parsed = new URI(path);
    } catch (URISyntaxException failure) {
      throw new IllegalArgumentException("Load path is not a valid URI: " + path, failure);
    }
    URI resolved = parsed;
    if (!parsed.isAbsolute()) {
      if (!baseUri.isPresent()) {
        throw new IllegalArgumentException(
            "Load path must be absolute when the base URI is absent: " + path);
      }
      resolved = baseUri.get().resolve(parsed);
    }
    if (!resolved.isAbsolute()) {
      throw new IllegalArgumentException("Load path did not resolve to an absolute URI: " + path);
    }
    return resolved.toString();
  }

  private static CloseableIterator<FilteredColumnarBatch> openScan(
      Load load, PreparedFile file, Engine engine, ExecutorService ioExecutor) {
    if (load.getFileType() == FileType.PARQUET) {
      return FileScanExecutor.execute((ScanParquet) file.scan, engine, ioExecutor);
    }
    return FileScanExecutor.execute((ScanJson) file.scan, engine, ioExecutor);
  }

  private static Future<RoaringBitmapArray> submitDeletionVector(
      Load load, PreparedFile file, Engine engine, ExecutorService ioExecutor) {
    if (file.deletionVector == null) {
      return CompletableFuture.completedFuture(null);
    }
    String tableRoot = load.getBaseUri().map(URI::toString).orElse("");
    return ioExecutor.submit(
        () -> {
          Tuple2<DeletionVectorDescriptor, RoaringBitmapArray> loaded =
              DeletionVectorUtils.loadNewDvAndBitmap(engine, tableRoot, file.deletionVector);
          return loaded._2;
        });
  }

  private static CloseableIterator<FilteredColumnarBatch> applyDeletionVector(
      PreparedFile file,
      CloseableIterator<FilteredColumnarBatch> reader,
      RoaringBitmapArray deletionVector) {
    return reader.map(batch -> withDeletionVector(batch, file.scanSchema, deletionVector));
  }

  private static FilteredColumnarBatch withDeletionVector(
      FilteredColumnarBatch batch, ScanSchema scanSchema, RoaringBitmapArray bitmap) {
    requireNonNull(batch, "Load reader batch is null");
    ColumnarBatch data = batch.getData();
    ColumnVector rowIndices = data.getColumnVector(scanSchema.rowIndexOrdinal);
    SelectionColumnVector selection =
        scanSchema.dropRowIndex
            ? new SelectionColumnVector(bitmap, rowIndices)
            : SelectionColumnVector.borrowing(bitmap, rowIndices);
    ColumnarBatch output =
        scanSchema.dropRowIndex ? data.withDeletedColumnAt(scanSchema.rowIndexOrdinal) : data;
    return new FilteredColumnarBatch(output, Optional.of(selection));
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

  private static void validateScan(Load load, StructType schema) {
    FileScan scan = newScan(load, null, schema);
    if (scan instanceof ScanParquet) {
      FileScanExecutor.validate((ScanParquet) scan);
    } else {
      FileScanExecutor.validate((ScanJson) scan);
    }
  }

  private static FileScan newScan(Load load, ScanFile file, StructType schema) {
    List<ScanFile> files = file == null ? Collections.emptyList() : Collections.singletonList(file);
    if (load.getFileType() == FileType.PARQUET) {
      return new ScanParquet(files, load.getFileConstantColumns(), schema);
    }
    return new ScanJson(files, load.getFileConstantColumns(), schema);
  }

  private static final class BoundLoad {
    private final StructType inputSchema;
    private final DefaultExpressionEvaluator path;
    private final DefaultExpressionEvaluator size;
    private final DefaultExpressionEvaluator deletionVector;
    private final StructType constantsSchema;
    private final int[] constantOrdinals;

    private BoundLoad(
        StructType inputSchema,
        DefaultExpressionEvaluator path,
        DefaultExpressionEvaluator size,
        DefaultExpressionEvaluator deletionVector,
        StructType constantsSchema,
        int[] constantOrdinals) {
      this.inputSchema = inputSchema;
      this.path = path;
      this.size = size;
      this.deletionVector = deletionVector;
      this.constantsSchema = constantsSchema;
      this.constantOrdinals = constantOrdinals;
    }

    static BoundLoad bind(Load load, StructType inputSchema) {
      List<StructField> constantFields = new ArrayList<>();
      int[] ordinals = new int[load.getFileConstantColumns().size()];
      for (int slot = 0; slot < ordinals.length; slot++) {
        String name = load.getFileConstantColumns().get(slot);
        ordinals[slot] = inputSchema.indexOf(name);
        constantFields.add(load.getSchema().get(name));
      }
      return new BoundLoad(
          inputSchema,
          new DefaultExpressionEvaluator(
              inputSchema, load.getFileMeta().getPathColumn(), StringType.STRING),
          new DefaultExpressionEvaluator(
              inputSchema, load.getFileMeta().getFileSizeColumn(), LongType.LONG),
          new DefaultExpressionEvaluator(
              inputSchema, load.getDvColumn(), DeletionVectorDescriptor.READ_SCHEMA),
          new StructType(constantFields),
          ordinals);
    }

    Row materializeConstants(ColumnarBatch data, int rowId) {
      Row row = new ColumnarBatchRow(data, rowId);
      List<Object> values = new ArrayList<>(constantOrdinals.length);
      for (int slot = 0; slot < constantOrdinals.length; slot++) {
        values.add(
            PlanValueUtils.read(
                row, constantsSchema.at(slot).getDataType(), constantOrdinals[slot]));
      }
      return GenericRow.fromValues(constantsSchema, values);
    }
  }

  private static final class MaterializedFile {
    private final Future<FileStatus> status;
    private final Row constants;
    private final DeletionVectorDescriptor deletionVector;

    private MaterializedFile(
        Future<FileStatus> status, Row constants, DeletionVectorDescriptor deletionVector) {
      this.status = status;
      this.constants = constants;
      this.deletionVector = deletionVector;
    }
  }

  private static final class PreparedFile {
    private final FileScan scan;
    private final DeletionVectorDescriptor deletionVector;
    private final ScanSchema scanSchema;

    private PreparedFile(
        FileScan scan, DeletionVectorDescriptor deletionVector, ScanSchema scanSchema) {
      this.scan = scan;
      this.deletionVector = deletionVector;
      this.scanSchema = scanSchema;
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
}
