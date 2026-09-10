/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.internal.parquet.ParquetFileReader;
import io.delta.kernel.defaults.internal.parquet.ParquetFileWriter;
import io.delta.kernel.defaults.internal.plans.FileScanExecutor;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.engine.ParquetHandler;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.plans.ScanParquet;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.*;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.LogStore;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

/** Default implementation of {@link ParquetHandler} based on Hadoop APIs. */
public class DefaultParquetHandler implements ParquetHandler {
  private static final String READER_PARALLELISM =
      "delta.kernel.default.parquet.reader.parallelism";

  private final FileIO fileIO;
  private final int readerParallelism;

  /**
   * Create an instance of default {@link ParquetHandler} implementation.
   *
   * @param fileIO File IO implementation to use for reading and writing files.
   */
  public DefaultParquetHandler(FileIO fileIO) {
    this.fileIO = Objects.requireNonNull(fileIO, "fileIO is null");
    this.readerParallelism =
        fileIO
            .getConf(READER_PARALLELISM)
            .map(Integer::parseInt)
            .orElse(Math.min(8, Runtime.getRuntime().availableProcessors()));
    if (readerParallelism <= 0) {
      throw new IllegalArgumentException(READER_PARALLELISM + " must be positive");
    }
  }

  @Override
  public CloseableIterator<FilteredColumnarBatch> readParquetFiles(ScanParquet scan) {
    return FileScanExecutor.execute(scan, this, new DefaultFileSystemClient(fileIO));
  }

  @Override
  public CloseableIterator<FileReadResult> readParquetFiles(
      CloseableIterator<FileStatus> fileIter,
      StructType physicalSchema,
      Optional<Predicate> predicate)
      throws IOException {
    if (readerParallelism > 1) {
      return OrderedParallelFileReader.read(
          fileIter,
          readerParallelism,
          file ->
              new ParquetFileReader(fileIO)
                  .read(file, physicalSchema, predicate)
                  .map(batch -> new FileReadResult(batch, file.getPath())));
    }
    return new CloseableIterator<FileReadResult>() {
      private final ParquetFileReader batchReader = new ParquetFileReader(fileIO);
      private CloseableIterator<ColumnarBatch> currentFileReader;
      private String currentFilePath;

      @Override
      public void close() throws IOException {
        Utils.closeCloseables(currentFileReader, fileIter);
      }

      @Override
      public boolean hasNext() {
        if (currentFileReader != null && currentFileReader.hasNext()) {
          return true;
        } else {
          // There is no file in reading or the current file being read has no more data.
          // Initialize the next file reader or return false if there are no more files to
          // read.
          Utils.closeCloseables(currentFileReader);
          currentFileReader = null;
          currentFilePath = null;
          if (fileIter.hasNext()) {
            FileStatus fileStatus = fileIter.next();
            currentFileReader = batchReader.read(fileStatus, physicalSchema, predicate);
            currentFilePath = fileStatus.getPath();
            return hasNext(); // recurse since it's possible the loaded file is empty
          } else {
            return false;
          }
        }
      }

      @Override
      public FileReadResult next() {
        return new FileReadResult(currentFileReader.next(), currentFilePath);
      }
    };
  }

  @Override
  public CloseableIterator<DataFileStatus> writeParquetFiles(
      String directoryPath,
      CloseableIterator<FilteredColumnarBatch> dataIter,
      List<Column> statsColumns)
      throws IOException {
    ParquetFileWriter batchWriter =
        ParquetFileWriter.multiFileWriter(fileIO, directoryPath, statsColumns);
    return batchWriter.write(dataIter);
  }

  /**
   * Makes use of {@link LogStore} implementations in `delta-storage` to atomically write the data
   * to a file depending upon the destination filesystem.
   *
   * @param filePath Fully qualified destination file path
   * @param data Iterator of {@link FilteredColumnarBatch}
   * @throws IOException
   */
  @Override
  public void writeParquetFileAtomically(
      String filePath, CloseableIterator<FilteredColumnarBatch> data) throws IOException {

    try {
      ParquetFileWriter fileWriter =
          ParquetFileWriter.singleFileWriter(
              fileIO,
              filePath,
              /* atomicWrite= */ true,
              /* statsColumns= */ Collections.emptyList());
      fileWriter.write(data).next(); // TODO: fix this
    } catch (UncheckedIOException e) {
      throw e.getCause();
    } finally {
      Utils.closeCloseables(data);
    }
  }
}
