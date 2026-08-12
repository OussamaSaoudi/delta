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
package io.delta.kernel.defaults.engine.hadoopio;

import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.OutputFile;
import io.delta.kernel.defaults.internal.logstore.LogStoreProvider;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.LogStore;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/** Implementation of {@link FileIO} based on Hadoop APIs. */
public class HadoopFileIO implements FileIO {
  private final Configuration hadoopConf;

  public HadoopFileIO(Configuration hadoopConf) {
    this.hadoopConf = Objects.requireNonNull(hadoopConf, "hadoopConf is null");
  }

  @Override
  public CloseableIterator<FileStatus> listFrom(String filePath) throws IOException {
    Path path = new Path(filePath);
    LogStore logStore = LogStoreProvider.getLogStore(hadoopConf, path.toUri().getScheme());

    return Utils.toCloseableIterator(logStore.listFrom(path, hadoopConf))
        .map(
            hadoopFileStatus ->
                FileStatus.of(
                    hadoopFileStatus.getPath().toString(),
                    hadoopFileStatus.getLen(),
                    hadoopFileStatus.getModificationTime()));
  }

  @Override
  public CloseableIterator<FileStatus> listFromRecursively(String filePath) throws IOException {
    Objects.requireNonNull(filePath, "filePath is null");

    Path requestedPath = new Path(filePath);
    FileSystem fs = requestedPath.getFileSystem(hadoopConf);
    Path qualifiedOffset = fs.makeQualified(requestedPath);
    Path listingRoot = filePath.endsWith("/") ? qualifiedOffset : qualifiedOffset.getParent();
    if (listingRoot == null) {
      throw new IOException("Cannot determine parent directory for " + filePath);
    }

    String offset = qualifiedOffset.toString();
    List<FileStatus> files = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> listing = fs.listFiles(listingRoot, true /* recursive */);
    while (listing.hasNext()) {
      LocatedFileStatus status = listing.next();
      String qualifiedPath = fs.makeQualified(status.getPath()).toString();
      if (compareUtf8(qualifiedPath, offset) > 0) {
        files.add(FileStatus.of(qualifiedPath, status.getLen(), status.getModificationTime()));
      }
    }
    files.sort((left, right) -> compareUtf8(left.getPath(), right.getPath()));
    return Utils.toCloseableIterator(files.iterator());
  }

  @Override
  public FileStatus getFileStatus(String path) throws IOException {
    Path pathObject = new Path(path);
    FileSystem fs = pathObject.getFileSystem(hadoopConf);
    org.apache.hadoop.fs.FileStatus hadoopFileStatus = fs.getFileStatus(pathObject);
    return FileStatus.of(
        hadoopFileStatus.getPath().toString(),
        hadoopFileStatus.getLen(),
        hadoopFileStatus.getModificationTime());
  }

  @Override
  public String resolvePath(String path) throws IOException {
    Path pathObject = new Path(path);
    FileSystem fs = pathObject.getFileSystem(hadoopConf);
    return fs.makeQualified(pathObject).toString();
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    Path pathObject = new Path(path);
    FileSystem fs = pathObject.getFileSystem(hadoopConf);
    return fs.mkdirs(pathObject);
  }

  @Override
  public InputFile newInputFile(String path, long fileSize) {
    return new HadoopInputFile(getFs(path), new Path(path), fileSize);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new HadoopOutputFile(hadoopConf, path);
  }

  @Override
  public void writeBytes(String path, byte[] data, boolean overwrite) throws IOException {
    Objects.requireNonNull(path, "path is null");
    Objects.requireNonNull(data, "data is null");
    Path target = new Path(path);
    FileSystem fs = target.getFileSystem(hadoopConf);
    try (FSDataOutputStream output = fs.create(target, overwrite)) {
      output.write(data);
    } catch (org.apache.hadoop.fs.FileAlreadyExistsException failure) {
      java.nio.file.FileAlreadyExistsException normalized =
          new java.nio.file.FileAlreadyExistsException(path);
      normalized.initCause(failure);
      throw normalized;
    }
  }

  @Override
  public boolean delete(String path) throws IOException {
    FileSystem fs = getFs(path);
    return fs.delete(new Path(path), false);
  }

  @Override
  public Optional<String> getConf(String confKey) {
    return Optional.ofNullable(hadoopConf.get(confKey));
  }

  @Override
  public void copyFileAtomically(String srcPath, String destPath, boolean overwrite)
      throws IOException {
    Path parsedSrcPath = new Path(srcPath);
    Path parsedDestPath = new Path(destPath);
    LogStore logStore = LogStoreProvider.getLogStore(hadoopConf, parsedSrcPath.toUri().getScheme());

    try (io.delta.storage.CloseableIterator<String> srcContents =
        logStore.read(parsedSrcPath, hadoopConf)) {
      logStore.write(parsedDestPath, srcContents, overwrite, hadoopConf);
    }
  }

  private FileSystem getFs(String path) {
    try {
      Path pathObject = new Path(path);
      return pathObject.getFileSystem(hadoopConf);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not resolve the FileSystem", e);
    }
  }

  private static int compareUtf8(String left, String right) {
    byte[] leftBytes = left.getBytes(StandardCharsets.UTF_8);
    byte[] rightBytes = right.getBytes(StandardCharsets.UTF_8);
    int sharedLength = Math.min(leftBytes.length, rightBytes.length);
    for (int index = 0; index < sharedLength; index++) {
      int comparison = Byte.compareUnsigned(leftBytes[index], rightBytes[index]);
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(leftBytes.length, rightBytes.length);
  }
}
