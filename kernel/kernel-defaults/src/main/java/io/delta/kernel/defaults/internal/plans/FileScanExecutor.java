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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

  private static <T> CloseableIterator<T> emptyIterator() {
    return toCloseableIterator(Collections.emptyIterator());
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
