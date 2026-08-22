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
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.plans.Load;
import io.delta.kernel.internal.plans.ScanFile;
import io.delta.kernel.internal.plans.ScanJson;
import io.delta.kernel.internal.plans.ScanParquet;
import io.delta.kernel.internal.util.ColumnBinding;
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

/** Executes a {@link Load} using Kernel Java file readers and deletion-vector utilities. */
final class LoadExecutor {
  private LoadExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(
      Load load,
      StructType inputSchema,
      CloseableIterator<FilteredColumnarBatch> input,
      Engine engine) {
    requireNonNull(load, "load is null");
    requireNonNull(inputSchema, "inputSchema is null");
    requireNonNull(input, "input is null");
    requireNonNull(engine, "engine is null");

    load.getOutputSchema(Collections.singletonList(inputSchema));
    BoundLoad bound = BoundLoad.bind(load, inputSchema);
    List<ScanFile> files = materializeFiles(load, bound, input);
    switch (load.getFileType()) {
      case PARQUET:
        return FileScanExecutor.execute(
            new ScanParquet(
                files,
                load.getFileConstantColumns(),
                load.getSchema(),
                load.getBaseUri()),
            engine);
      case JSON:
        return FileScanExecutor.execute(
            new ScanJson(
                files,
                load.getFileConstantColumns(),
                load.getSchema(),
                load.getBaseUri()),
            engine);
      default:
        throw new IllegalStateException("Unsupported Load file type: " + load.getFileType());
    }
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
    ColumnVector paths = bound.path.getVector(data);
    ColumnVector sizes = bound.size.getVector(data);
    ColumnVector deletionVectors = bound.deletionVector.getVector(data);
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
    private final ColumnBinding path;
    private final ColumnBinding size;
    private final ColumnBinding deletionVector;
    private final StructType constantsSchema;
    private final int[] constantOrdinals;

    private BoundLoad(
        StructType inputSchema,
        ColumnBinding path,
        ColumnBinding size,
        ColumnBinding deletionVector,
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
          ColumnBinding.resolve(inputSchema, load.getFileMeta().getPathColumn()),
          ColumnBinding.resolve(inputSchema, load.getFileMeta().getFileSizeColumn()),
          ColumnBinding.resolve(inputSchema, load.getDvColumn()),
          new StructType(constantFields),
          ordinals);
    }

    Row materializeConstants(ColumnarBatch data, int rowId) {
      List<Object> values = new ArrayList<>(constantOrdinals.length);
      for (int slot = 0; slot < constantOrdinals.length; slot++) {
        values.add(
            PlanValueUtils.materialize(
                data.getColumnVector(constantOrdinals[slot]),
                constantsSchema.at(slot).getDataType(),
                rowId));
      }
      return GenericRow.fromValues(constantsSchema, values);
    }
  }
}
