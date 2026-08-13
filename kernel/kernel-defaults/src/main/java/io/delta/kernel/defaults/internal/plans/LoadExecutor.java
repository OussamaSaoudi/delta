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
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.ColumnarBatchRow;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.plans.Load;
import io.delta.kernel.internal.plans.ScanFile;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.LongType;
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
import java.util.concurrent.ExecutorService;

/** Executes a {@link Load} using Kernel Java file readers and deletion-vector utilities. */
final class LoadExecutor {
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
    FileScanExecutor.validate(
        load.getFileType(), load.getSchema(), load.getFileConstantColumns(), false);
    FileScanExecutor.validate(
        load.getFileType(), load.getSchema(), load.getFileConstantColumns(), true);
    BoundLoad bound = BoundLoad.bind(load, inputSchema);
    List<ScanFile> files = materializeFiles(load, bound, input);
    return FileScanExecutor.execute(
        load.getFileType(),
        load.getSchema(),
        load.getFileConstantColumns(),
        load.getBaseUri(),
        files,
        engine,
        ioExecutor);
  }

  private static List<ScanFile> materializeFiles(
      Load load, BoundLoad bound, CloseableIterator<FilteredColumnarBatch> input) {
    List<ScanFile> files = new ArrayList<>();
    try (CloseableIterator<FilteredColumnarBatch> batches = input) {
      while (batches.hasNext()) {
        FilteredColumnarBatch batch = requireNonNull(batches.next(), "Load input batch is null");
        ColumnarBatch data = batch.getData();
        checkArgument(
            bound.inputSchema.equals(data.getSchema()),
            "Load input batch schema %s does not match expected schema %s",
            data.getSchema(),
            bound.inputSchema);
        materializeBatch(load, bound, batch, files);
      }
      return files;
    } catch (IOException failure) {
      throw new UncheckedIOException("Failed to close Load input", failure);
    }
  }

  private static void materializeBatch(
      Load load, BoundLoad bound, FilteredColumnarBatch batch, List<ScanFile> files) {
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
        FileStatus knownStatus = knownFileStatus(location, sizes, rowId);
        Optional<DeletionVectorDescriptor> deletionVector = Optional.ofNullable(dv);
        files.add(
            knownStatus == null
                ? new ScanFile(location, constants, deletionVector)
                : new ScanFile(knownStatus, constants, deletionVector));
      }
    } finally {
      Utils.closeCloseables(paths, sizes, deletionVectors);
    }
  }

  private static FileStatus knownFileStatus(String location, ColumnVector sizes, int rowId) {
    if (sizes.isNullAt(rowId)) {
      return null;
    }
    long size = sizes.getLong(rowId);
    if (size < 0) {
      throw new IllegalArgumentException("Load file size must be non-negative");
    }
    return FileStatus.of(location, size, 0);
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
}
