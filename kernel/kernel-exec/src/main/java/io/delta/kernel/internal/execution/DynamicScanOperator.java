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
package io.delta.kernel.internal.execution;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.ColumnBinding;
import io.delta.kernel.internal.util.RowKernels;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.plans.DynamicScan;
import io.delta.kernel.plans.FileType;
import io.delta.kernel.plans.ScanFile;
import io.delta.kernel.plans.ScanJson;
import io.delta.kernel.plans.ScanParquet;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

/** Materializes file metadata and dispatches the corresponding static scan. */
final class DynamicScanOperator {
  private DynamicScanOperator() {}

  static CloseableIterator<FilteredColumnarBatch> open(DynamicScan scan, ExecutionContext context) {
    BoundMetadata metadata = new BoundMetadata(scan);
    CloseableIterator<FilteredColumnarBatch> input = context.open(scan.fileMetadataInput());
    return new CloseableIterator<FilteredColumnarBatch>() {
      private CloseableIterator<FilteredColumnarBatch> output;
      private boolean closed;

      @Override
      public boolean hasNext() {
        requireOpen();
        initialize();
        boolean hasNext = output.hasNext();
        if (!hasNext) {
          close();
        }
        return hasNext;
      }

      @Override
      public FilteredColumnarBatch next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return output.next();
      }

      @Override
      public void close() {
        if (closed) {
          return;
        }
        closed = true;
        Utils.closeCloseables(input, output);
      }

      private void initialize() {
        if (output != null) {
          return;
        }
        List<ScanFile> files = metadata.materialize(input);
        if (scan.fileType() == FileType.JSON) {
          ScanJson staticScan =
              new ScanJson(
                  files,
                  Optional.of(scan.tableRoot()),
                  scan.fileConstantColumns(),
                  scan.dataSchema());
          output = ScanOperator.open(staticScan, context.engine());
        } else if (scan.fileType() == FileType.PARQUET) {
          ScanParquet staticScan =
              new ScanParquet(
                  files,
                  Optional.of(scan.tableRoot()),
                  scan.fileConstantColumns(),
                  scan.dataSchema(),
                  Optional.empty());
          output = ScanOperator.open(staticScan, context.engine());
        } else {
          throw new UnsupportedOperationException("Unsupported scan type: " + scan.fileType());
        }
      }

      private void requireOpen() {
        if (closed) {
          throw new IllegalStateException("Dynamic scan is closed");
        }
      }
    };
  }

  private static final class BoundMetadata {
    private final DynamicScan scan;
    private final ColumnBinding path;
    private final ColumnBinding size;
    private final ColumnBinding modified;
    private final ColumnBinding deletionVector;
    private final StructType constantsSchema;
    private final int[] constantOrdinals;

    private BoundMetadata(DynamicScan scan) {
      this.scan = scan;
      StructType inputSchema = scan.fileMetadataInput().outputSchema();
      this.path = ColumnBinding.resolve(inputSchema, scan.pathColumn());
      this.size = ColumnBinding.resolve(inputSchema, scan.fileSizeColumn());
      this.modified = ColumnBinding.resolve(inputSchema, scan.lastModifiedColumn());
      this.deletionVector = ColumnBinding.resolve(inputSchema, scan.deletionVectorColumn());
      List<StructField> fields = new ArrayList<>(scan.fileConstantColumns().size());
      this.constantOrdinals = new int[scan.fileConstantColumns().size()];
      for (int index = 0; index < constantOrdinals.length; index++) {
        String name = scan.fileConstantColumns().get(index);
        constantOrdinals[index] = inputSchema.indexOf(name);
        fields.add(scan.dataSchema().get(name));
      }
      this.constantsSchema = new StructType(fields);
    }

    private List<ScanFile> materialize(CloseableIterator<FilteredColumnarBatch> input) {
      List<ScanFile> files = new ArrayList<>();
      try (CloseableIterator<FilteredColumnarBatch> batches = input) {
        while (batches.hasNext()) {
          FilteredColumnarBatch batch = batches.next();
          materializeBatch(batch, files);
        }
      } catch (RuntimeException failure) {
        throw failure;
      } catch (Exception failure) {
        throw new RuntimeException("Could not close dynamic scan metadata", failure);
      }
      return files;
    }

    private void materializeBatch(FilteredColumnarBatch batch, List<ScanFile> files) {
      ColumnarBatch data = batch.getData();
      ColumnVector paths = path.getVector(data);
      ColumnVector sizes = size.getVector(data);
      ColumnVector modifiedTimes = modified.getVector(data);
      ColumnVector deletionVectors = deletionVector.getVector(data);
      for (int rowId = 0; rowId < data.getSize(); rowId++) {
        if (!batch.isSelected(rowId)) {
          continue;
        }
        if (paths.isNullAt(rowId) || sizes.isNullAt(rowId) || modifiedTimes.isNullAt(rowId)) {
          throw new IllegalArgumentException(
              "Dynamic scan path, size, and modification time must be non-null");
        }
        String resolved = resolve(scan.tableRoot(), paths.getString(rowId));
        FileStatus status =
            FileStatus.of(resolved, sizes.getLong(rowId), modifiedTimes.getLong(rowId));
        DeletionVectorDescriptor dv =
            DeletionVectorDescriptor.fromColumnVector(deletionVectors, rowId);
        files.add(new ScanFile(status, materializeConstants(data, rowId), Optional.ofNullable(dv)));
      }
    }

    private Row materializeConstants(ColumnarBatch data, int rowId) {
      Object[] values = new Object[constantOrdinals.length];
      for (int index = 0; index < values.length; index++) {
        values[index] =
            RowKernels.materialize(
                data.getColumnVector(constantOrdinals[index]),
                constantsSchema.at(index).getDataType(),
                rowId);
      }
      return GenericRow.fromOwnedValues(constantsSchema, values);
    }

    private static String resolve(URI tableRoot, String path) {
      try {
        URI parsed = new URI(path);
        return (parsed.isAbsolute() ? parsed : tableRoot.resolve(parsed)).toString();
      } catch (URISyntaxException failure) {
        throw new IllegalArgumentException("Invalid dynamic scan path: " + path, failure);
      }
    }
  }
}
