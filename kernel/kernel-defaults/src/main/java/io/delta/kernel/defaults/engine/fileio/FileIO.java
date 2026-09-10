/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.engine.fileio;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

/**
 * Interface for file IO operations. Connectors can implement their own version of the {@link
 * FileIO} depending upon their environment. The {@link DefaultEngine} takes {@link FileIO} instance
 * as input and all I/O operations from the default engine are done using the passed in {@link
 * FileIO} instance.
 */
public interface FileIO {
  /**
   * List the paths in the same directory that are lexicographically greater or equal to (UTF-8
   * sorting) the given `path`. The result should also be sorted by the file name.
   *
   * @param filePath Fully qualified path to a file
   * @return Closeable iterator of files. It is the responsibility of the caller to close the
   *     iterator.
   * @throws FileNotFoundException if the file at the given path is not found
   * @throws IOException for any other IO error.
   */
  CloseableIterator<FileStatus> listFrom(String filePath) throws IOException;

  /**
   * Recursively list files whose fully qualified paths are lexicographically greater than the given
   * path, using unsigned UTF-8 byte ordering.
   *
   * <p>A path ending in {@code /} is treated as a directory and lists all files below it.
   * Otherwise, the listing is rooted at the path's parent directory. Results must be strictly
   * greater than the input path and sorted by full path.
   *
   * <p>The default preserves compatibility with existing {@link FileIO} implementations. File IO
   * implementations used for plan-based execution must override it.
   *
   * @param filePath Fully qualified file offset or directory path
   * @return Closeable iterator of recursively listed files. The caller must close the iterator.
   * @throws FileNotFoundException if the directory to list does not exist
   * @throws IOException for any other IO error
   * @throws UnsupportedOperationException if recursive listing is not implemented
   */
  default CloseableIterator<FileStatus> listFromRecursively(String filePath) throws IOException {
    throw new UnsupportedOperationException("Recursive file listing is not supported");
  }

  /**
   * Get the metadata of the file at the given path.
   *
   * @param path Fully qualified path to the file.
   * @return Metadata of the file.
   * @throws IOException for any IO error.
   */
  FileStatus getFileStatus(String path) throws IOException;

  /**
   * Resolve the given path to a fully qualified path.
   *
   * @param path Input path
   * @return Fully qualified path.
   * @throws FileNotFoundException If the given path doesn't exist.
   * @throws IOException for any other IO error.
   */
  String resolvePath(String path) throws IOException;

  /**
   * Create a directory at the given path including parent directories. This mimics the behavior of
   * `mkdir -p` in Unix.
   *
   * @param path Full qualified path to create a directory at.
   * @return true if the directory was created successfully, false otherwise.
   * @throws IOException for any IO error.
   */
  boolean mkdirs(String path) throws IOException;

  /**
   * Get an {@link InputFile} for file at given path which can be used to read the file from any
   * arbitrary position in the file.
   *
   * @param path Fully qualified path to the file.
   * @param fileSize Size of the file in bytes.
   * @return {@link InputFile} instance.
   */
  InputFile newInputFile(String path, long fileSize);

  /**
   * Create a {@link OutputFile} to write new file at the given path.
   *
   * @param path Fully qualified path to the file.
   * @return {@link OutputFile} instance which can be used to write to the file.
   */
  OutputFile newOutputFile(String path);

  /**
   * Write raw bytes to a file.
   *
   * <p>If {@code overwrite} is false, the implementation must atomically fail instead of replacing
   * a file that already exists. The default preserves compatibility with existing implementations;
   * file IO used for plan-based storage execution must override it.
   *
   * @param path fully qualified path to write
   * @param data bytes to write
   * @param overwrite whether an existing file may be replaced
   * @throws java.nio.file.FileAlreadyExistsException if the file exists and overwrite is false
   * @throws IOException for any other I/O error
   * @throws UnsupportedOperationException if raw byte writes are not implemented
   */
  default void writeBytes(String path, byte[] data, boolean overwrite) throws IOException {
    throw new UnsupportedOperationException("Raw byte writes are not supported");
  }

  /**
   * Delete the file at given path.
   *
   * @param path the path to delete. If path is a directory throws an exception.
   * @return true if delete is successful else false.
   * @throws IOException for any IO error.
   */
  boolean delete(String path) throws IOException;

  /**
   * Get the configuration value for the given key.
   *
   * <p>TODO: should be in a separate interface? may be called ConfigurationProvider?
   *
   * @param confKey configuration key name
   * @return If no such value is present, an {@link Optional#empty()} is returned.
   */
  Optional<String> getConf(String confKey);

  /**
   * Atomically copy a file from source path to destination path. The copy operation should be
   * atomic to ensure that the destination file is either fully copied or not present at all.
   *
   * @param srcPath Fully qualified path to the source file to copy
   * @param destPath Fully qualified path to the destination where the file will be copied
   * @param overwrite If true, overwrite the destination file if it already exists. If false, throw
   *     an exception if the destination exists.
   * @throws java.nio.file.FileAlreadyExistsException if the destination file already exists and
   *     {@code overwrite} is false.
   * @throws FileNotFoundException if the source file does not exist
   * @throws IOException for any other IO error
   */
  void copyFileAtomically(String srcPath, String destPath, boolean overwrite) throws IOException;
}
