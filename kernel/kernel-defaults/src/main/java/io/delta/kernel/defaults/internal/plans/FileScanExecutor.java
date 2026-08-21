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
import java.util.List;
import java.util.Optional;

/** Executes ordered file scans through the existing Kernel Java handlers. */
final class FileScanExecutor {
  private FileScanExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(ScanParquet scan, Engine engine) {
    requireNonNull(scan, "scan is null");
    requireNonNull(engine, "engine is null");
    StructType readSchema = readSchema(scan, false);
    return scanFiles(scan)
        .flatMap(
            file ->
                openParquet(file, readSchema, engine)
                    .map(
                        result ->
                            filtered(splice(scan, file, result.getData(), 0, false, readSchema))));
  }

  static CloseableIterator<FilteredColumnarBatch> execute(ScanJson scan, Engine engine) {
    requireNonNull(scan, "scan is null");
    requireNonNull(engine, "engine is null");
    StructType readSchema = readSchema(scan, true);
    return scanFiles(scan).flatMap(file -> openJson(scan, file, readSchema, engine));
  }

  private static CloseableIterator<FileReadResult> openParquet(
      ScanFile file, StructType readSchema, Engine engine) {
    return openReader(
        file,
        "Parquet",
        statuses ->
            engine
                .getParquetHandler()
                .readParquetFiles(statuses, readSchema, Optional.empty()));
  }

  private static CloseableIterator<FilteredColumnarBatch> openJson(
      ScanJson scan, ScanFile file, StructType readSchema, Engine engine) {
    CloseableIterator<ColumnarBatch> reader =
        openReader(
            file,
            "JSON",
            statuses ->
                engine.getJsonHandler().readJsonFiles(statuses, readSchema, Optional.empty()));
    long[] nextRowIndex = {0};
    return reader.map(
        batch -> {
          long rowIndex = nextRowIndex[0];
          nextRowIndex[0] = Math.addExact(rowIndex, batch.getSize());
          return filtered(splice(scan, file, batch, rowIndex, true, readSchema));
        });
  }

  private static <T> CloseableIterator<T> openReader(
      ScanFile file, String format, ReaderOpener<T> opener) {
    CloseableIterator<FileStatus> status =
        toCloseableIterator(Collections.singleton(file.getFileStatus()).iterator());
    try {
      return requireNonNull(opener.open(status), format + " reader is null");
    } catch (IOException failure) {
      Utils.closeCloseablesSilently(status);
      throw new UncheckedIOException("Failed to open " + format + " scan", failure);
    } catch (RuntimeException | Error failure) {
      Utils.closeCloseablesSilently(status);
      throw failure;
    }
  }

  private static StructType readSchema(FileScan scan, boolean json) {
    List<StructField> fields = new ArrayList<>();
    for (StructField field : scan.getSchema().fields()) {
      if (scan.getFileConstantColumns().contains(field.getName())) {
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
      FileScan scan,
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
    for (int ordinal = 0; ordinal < scan.getSchema().length(); ordinal++) {
      StructField field = scan.getSchema().at(ordinal);
      int constant = scan.getFileConstantColumns().indexOf(field.getName());
      if (constant >= 0) {
        ColumnVector vector =
            new DefaultSubFieldVector(
                batch.getSize(),
                field.getDataType(),
                constant,
                ignored -> file.getFileConstants());
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

  private static CloseableIterator<ScanFile> scanFiles(FileScan scan) {
    return toCloseableIterator(scan.getFiles().iterator());
  }

  private static FilteredColumnarBatch filtered(ColumnarBatch batch) {
    return new FilteredColumnarBatch(batch, Optional.empty());
  }

  @FunctionalInterface
  private interface ReaderOpener<T> {
    CloseableIterator<T> open(CloseableIterator<FileStatus> statuses) throws IOException;
  }
}
