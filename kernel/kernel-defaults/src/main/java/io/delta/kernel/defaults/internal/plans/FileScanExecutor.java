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
import io.delta.kernel.data.FilteredColumnarBatch.Lifetime;
import io.delta.kernel.defaults.internal.data.vector.DefaultLongVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultSubFieldVector;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.engine.FileSystemClient;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.engine.ParquetHandler;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.SelectionColumnVector;
import io.delta.kernel.internal.deletionvectors.DeletionVectorStoredBitmap;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.plans.FileScan;
import io.delta.kernel.plans.ScanFile;
import io.delta.kernel.plans.ScanJson;
import io.delta.kernel.plans.ScanParquet;
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

/** Executes plan scans through the existing Kernel Java format handlers. */
public final class FileScanExecutor {
  private static final String PRIVATE_ROW_INDEX = "__delta_kernel_scan_row_index";

  private FileScanExecutor() {}

  public static CloseableIterator<FilteredColumnarBatch> execute(
      ScanParquet scan, ParquetHandler parquet, FileSystemClient fileSystem) {
    requireNonNull(scan, "scan is null");
    requireNonNull(parquet, "parquet is null");
    requireNonNull(fileSystem, "fileSystem is null");

    StructType outputSchema = outputSchema(scan);
    StructType readSchema = readSchema(scan, outputSchema, false);
    boolean dropRowIndex = requiresPrivateRowIndex(scan);
    if (hasDuplicatePaths(scan.files())) {
      return scanFiles(scan)
          .flatMap(
              file -> {
                RoaringBitmapArray bitmap = loadDeletionVector(scan, file, fileSystem);
                return openParquet(scan, file, readSchema, parquet)
                    .map(
                        result ->
                            finishBatch(
                                file,
                                bitmap,
                                splice(
                                    outputSchema,
                                    scan.fileConstantColumns(),
                                    file,
                                    result.getData(),
                                    0,
                                    false,
                                    readSchema),
                                dropRowIndex));
              });
    }

    Map<String, ScanFile> filesByPath = new HashMap<>();
    for (ScanFile file : scan.files()) {
      filesByPath.put(file.getFileStatus().getPath(), file);
    }
    Map<String, RoaringBitmapArray> deletionVectors = new HashMap<>();
    return openParquet(scan, scan.files(), readSchema, parquet)
        .map(
            result -> {
              ScanFile file = filesByPath.get(result.getFilePath());
              if (file == null) {
                throw new IllegalArgumentException(
                    "Parquet reader returned an unknown file " + result.getFilePath());
              }
              RoaringBitmapArray bitmap =
                  deletionVectors.computeIfAbsent(
                      result.getFilePath(), ignored -> loadDeletionVector(scan, file, fileSystem));
              return finishBatch(
                  file,
                  bitmap,
                  splice(
                      outputSchema,
                      scan.fileConstantColumns(),
                      file,
                      result.getData(),
                      0,
                      false,
                      readSchema),
                  dropRowIndex);
            });
  }

  public static CloseableIterator<FilteredColumnarBatch> execute(
      ScanJson scan, JsonHandler json, FileSystemClient fileSystem) {
    requireNonNull(scan, "scan is null");
    requireNonNull(json, "json is null");
    requireNonNull(fileSystem, "fileSystem is null");

    StructType outputSchema = outputSchema(scan);
    StructType readSchema = readSchema(scan, outputSchema, true);
    boolean dropRowIndex = requiresPrivateRowIndex(scan);
    return scanFiles(scan)
        .flatMap(
            file -> openJson(scan, file, outputSchema, readSchema, dropRowIndex, json, fileSystem));
  }

  private static CloseableIterator<FileReadResult> openParquet(
      ScanParquet scan, ScanFile file, StructType readSchema, ParquetHandler parquet) {
    return openParquet(scan, Collections.singletonList(file), readSchema, parquet);
  }

  private static CloseableIterator<FileReadResult> openParquet(
      ScanParquet scan, List<ScanFile> files, StructType readSchema, ParquetHandler parquet) {
    CloseableIterator<FileStatus> statuses =
        toCloseableIterator(files.stream().map(ScanFile::getFileStatus).iterator());
    return openReader(
        statuses,
        "Parquet",
        input -> parquet.readParquetFiles(input, readSchema, scan.pushdownHint()));
  }

  private static CloseableIterator<FilteredColumnarBatch> openJson(
      ScanJson scan,
      ScanFile file,
      StructType outputSchema,
      StructType readSchema,
      boolean dropRowIndex,
      JsonHandler json,
      FileSystemClient fileSystem) {
    CloseableIterator<FileStatus> status =
        toCloseableIterator(Collections.singleton(file.getFileStatus()).iterator());
    CloseableIterator<ColumnarBatch> reader =
        openReader(
            status, "JSON", input -> json.readJsonFiles(input, readSchema, Optional.empty()));
    long[] nextRowIndex = {0};
    RoaringBitmapArray bitmap = loadDeletionVector(scan, file, fileSystem);
    return reader.map(
        batch -> {
          long rowIndex = nextRowIndex[0];
          nextRowIndex[0] = Math.addExact(rowIndex, batch.getSize());
          return finishBatch(
              file,
              bitmap,
              splice(
                  outputSchema,
                  scan.fileConstantColumns(),
                  file,
                  batch,
                  rowIndex,
                  true,
                  readSchema),
              dropRowIndex);
        });
  }

  private static <T> CloseableIterator<T> openReader(
      CloseableIterator<FileStatus> statuses, String format, ReaderOpener<T> opener) {
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

  private static StructType outputSchema(FileScan scan) {
    if (!requiresPrivateRowIndex(scan)) {
      return scan.outputSchema();
    }
    return scan.outputSchema()
        .add(StructField.createMetadataColumn(PRIVATE_ROW_INDEX, MetadataColumnSpec.ROW_INDEX));
  }

  private static boolean requiresPrivateRowIndex(FileScan scan) {
    return scan.outputSchema().indexOf(MetadataColumnSpec.ROW_INDEX) < 0
        && scan.files().stream().anyMatch(file -> file.getDeletionVector().isPresent());
  }

  private static RoaringBitmapArray loadDeletionVector(
      FileScan scan, ScanFile file, FileSystemClient fileSystem) {
    Optional<DeletionVectorDescriptor> descriptor = file.getDeletionVector();
    if (!descriptor.isPresent()) {
      return null;
    }
    try {
      return new DeletionVectorStoredBitmap(descriptor.get(), scan.tableRoot().map(URI::toString))
          .load(fileSystem);
    } catch (IOException failure) {
      throw new UncheckedIOException(
          "Failed to load deletion vector for " + file.getFileStatus().getPath(), failure);
    }
  }

  private static FilteredColumnarBatch finishBatch(
      ScanFile file, RoaringBitmapArray bitmap, ColumnarBatch batch, boolean dropRowIndex) {
    int rowIndexOrdinal = batch.getSchema().indexOf(MetadataColumnSpec.ROW_INDEX);
    if (bitmap == null) {
      if (!dropRowIndex) {
        return filtered(batch);
      }
      ColumnVector rowIndices = batch.getColumnVector(rowIndexOrdinal);
      ColumnarBatch output = batch.withDeletedColumnAt(rowIndexOrdinal);
      rowIndices.close();
      return filtered(output);
    }
    if (rowIndexOrdinal < 0) {
      throw new IllegalArgumentException(
          "Row index column is absent while applying deletion vector for "
              + file.getFileStatus().getPath());
    }

    ColumnVector rowIndices = batch.getColumnVector(rowIndexOrdinal);
    ColumnarBatch output = dropRowIndex ? batch.withDeletedColumnAt(rowIndexOrdinal) : batch;
    SelectionColumnVector selection =
        dropRowIndex
            ? new SelectionColumnVector(bitmap, rowIndices)
            : SelectionColumnVector.borrowing(bitmap, rowIndices);
    return new FilteredColumnarBatch(output, Optional.of(selection), Lifetime.OWNED);
  }

  private static StructType readSchema(FileScan scan, StructType outputSchema, boolean json) {
    List<StructField> fields = new ArrayList<>();
    for (StructField field : outputSchema.fields()) {
      if (scan.fileConstantColumns().contains(field.getName())) {
        continue;
      }
      if (json && field.isMetadataColumn()) {
        if (field.getMetadataColumnSpec() != MetadataColumnSpec.ROW_INDEX) {
          throw new IllegalArgumentException(
              "Unsupported JSON scan metadata column `" + field.getName() + "`");
        }
        continue;
      }
      fields.add(field);
    }
    return new StructType(fields);
  }

  private static ColumnarBatch splice(
      StructType outputSchema,
      List<String> fileConstantColumns,
      ScanFile file,
      ColumnarBatch batch,
      long jsonRowIndex,
      boolean json,
      StructType expectedReadSchema) {
    requireNonNull(batch, "reader batch is null");
    if (!expectedReadSchema.equals(batch.getSchema())) {
      throw new IllegalArgumentException(
          "Scan reader returned schema " + batch.getSchema() + ", expected " + expectedReadSchema);
    }

    ColumnarBatch output = batch;
    for (int ordinal = 0; ordinal < outputSchema.length(); ordinal++) {
      StructField field = outputSchema.at(ordinal);
      int constant = fileConstantColumns.indexOf(field.getName());
      if (constant >= 0) {
        ColumnVector vector =
            new DefaultSubFieldVector(
                batch.getSize(), field.getDataType(), constant, ignored -> file.getFileConstants());
        output = output.withNewColumn(ordinal, field, vector);
      } else if (json && field.isMetadataColumn()) {
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

  private static boolean hasDuplicatePaths(List<ScanFile> files) {
    Set<String> paths = new HashSet<>();
    for (ScanFile file : files) {
      if (!paths.add(file.getFileStatus().getPath())) {
        return true;
      }
    }
    return false;
  }

  private static CloseableIterator<ScanFile> scanFiles(FileScan scan) {
    return toCloseableIterator(scan.files().iterator());
  }

  private static FilteredColumnarBatch filtered(ColumnarBatch batch) {
    return new FilteredColumnarBatch(batch, Optional.empty(), Lifetime.OWNED);
  }

  @FunctionalInterface
  private interface ReaderOpener<T> {
    CloseableIterator<T> open(CloseableIterator<FileStatus> statuses) throws IOException;
  }
}
