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
import io.delta.storage.internal.PathLock;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/** Implementation of {@link FileIO} based on Hadoop APIs. */
public class HadoopFileIO implements FileIO {
  private static final int COPY_BUFFER_SIZE = 8192;
  private static final PathLock COPY_PATH_LOCK = new PathLock();

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
    Objects.requireNonNull(srcPath, "srcPath is null");
    Objects.requireNonNull(destPath, "destPath is null");
    Path parsedSrcPath = new Path(srcPath);
    Path parsedDestPath = new Path(destPath);
    FileSystem srcFs = parsedSrcPath.getFileSystem(hadoopConf);
    Path resolvedSrcPath = srcFs.resolvePath(parsedSrcPath);
    LogStore destLogStore =
        LogStoreProvider.getLogStore(hadoopConf, parsedDestPath.toUri().getScheme());
    Path resolvedDestPath = destLogStore.resolvePathOnPhysicalStorage(parsedDestPath, hadoopConf);

    if (resolvedSrcPath.equals(resolvedDestPath)) {
      if (!overwrite) {
        throw new java.nio.file.FileAlreadyExistsException(resolvedDestPath.toString());
      }
      return;
    }

    boolean locked = false;
    try (FSDataInputStream input = srcFs.open(resolvedSrcPath)) {
      try {
        COPY_PATH_LOCK.acquire(resolvedDestPath);
        locked = true;
      } catch (InterruptedException failure) {
        Thread.currentThread().interrupt();
        InterruptedIOException interrupted =
            new InterruptedIOException("Interrupted while copying to " + destPath);
        interrupted.initCause(failure);
        throw interrupted;
      }

      try {
        if (destLogStore.isPartialWriteVisible(resolvedDestPath, hadoopConf)) {
          copyWithAtomicRename(input, resolvedDestPath, overwrite);
        } else {
          copyToAtomicallyVisibleStore(input, resolvedDestPath, overwrite);
        }
      } catch (IOException failure) {
        throw normalizeCopyFailure(failure, resolvedDestPath, overwrite);
      }
    } finally {
      if (locked) {
        COPY_PATH_LOCK.release(resolvedDestPath);
      }
    }
  }

  private void copyWithAtomicRename(FSDataInputStream input, Path destPath, boolean overwrite)
      throws IOException {
    FileContext fileContext = FileContext.getFileContext(destPath.toUri(), hadoopConf);
    if (!overwrite && fileContext.util().exists(destPath)) {
      throw new java.nio.file.FileAlreadyExistsException(destPath.toString());
    }

    Path parent = destPath.getParent();
    if (parent == null) {
      throw new IOException("Cannot determine parent directory for " + destPath);
    }
    Path tempPath =
        new Path(parent, String.format(".%s.%s.tmp", destPath.getName(), UUID.randomUUID()));
    boolean renamed = false;
    Throwable primary = null;
    try {
      try (FSDataOutputStream output =
          fileContext.create(
              tempPath,
              EnumSet.of(CreateFlag.CREATE),
              Options.CreateOpts.checksumParam(Options.ChecksumOpt.createDisabled()))) {
        copyBytes(input, output);
      }
      fileContext.rename(
          tempPath, destPath, overwrite ? Options.Rename.OVERWRITE : Options.Rename.NONE);
      renamed = true;
    } catch (IOException | RuntimeException | Error failure) {
      primary = failure;
      throw failure;
    } finally {
      if (!renamed) {
        try {
          fileContext.delete(tempPath, false /* recursive */);
        } catch (IOException | RuntimeException | Error cleanupFailure) {
          if (primary == null) {
            throw cleanupFailure;
          }
          primary.addSuppressed(cleanupFailure);
        }
      }
    }
  }

  private void copyToAtomicallyVisibleStore(
      FSDataInputStream input, Path destPath, boolean overwrite) throws IOException {
    FileSystem destFs = destPath.getFileSystem(hadoopConf);
    if (!overwrite && destFs.exists(destPath)) {
      throw new java.nio.file.FileAlreadyExistsException(destPath.toString());
    }
    try (FSDataOutputStream output = destFs.create(destPath, overwrite)) {
      copyBytes(input, output);
    }
  }

  private static void copyBytes(java.io.InputStream input, java.io.OutputStream output)
      throws IOException {
    byte[] buffer = new byte[COPY_BUFFER_SIZE];
    int read;
    while ((read = input.read(buffer)) >= 0) {
      if (read > 0) {
        output.write(buffer, 0, read);
      }
    }
  }

  private static IOException normalizeCopyFailure(
      IOException failure, Path destPath, boolean overwrite) {
    if (!overwrite && isDestinationConflict(failure)) {
      java.nio.file.FileAlreadyExistsException normalized =
          new java.nio.file.FileAlreadyExistsException(destPath.toString());
      normalized.initCause(failure);
      return normalized;
    }
    return failure;
  }

  private static boolean isDestinationConflict(Throwable failure) {
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (current instanceof java.nio.file.FileAlreadyExistsException
          || current instanceof org.apache.hadoop.fs.FileAlreadyExistsException) {
        return true;
      }
      String message = current.getMessage();
      if (message != null && message.contains("412 Precondition Failed")) {
        return true;
      }
    }
    return false;
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
