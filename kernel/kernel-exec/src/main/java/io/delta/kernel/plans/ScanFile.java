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
package io.delta.kernel.plans;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.util.RowKernels;
import io.delta.kernel.utils.FileStatus;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Optional;

/** A fully resolved file, its constant values, and its optional deletion vector. */
public final class ScanFile {
  private final FileStatus fileStatus;
  private final Row fileConstants;
  private final Optional<DeletionVectorDescriptor> deletionVector;

  public ScanFile(
      FileStatus fileStatus, Row fileConstants, Optional<DeletionVectorDescriptor> deletionVector) {
    this.fileStatus = requireNonNull(fileStatus, "fileStatus is null");
    validatePath(fileStatus.getPath());
    if (fileStatus.getSize() <= 0) {
      throw new IllegalArgumentException("Scan file size must be positive");
    }
    this.fileConstants = requireNonNull(fileConstants, "fileConstants is null");
    this.deletionVector = requireNonNull(deletionVector, "deletionVector is null");
  }

  private static void validatePath(String path) {
    requireNonNull(path, "scan file path is null");
    try {
      if (!new URI(path).isAbsolute()) {
        throw new IllegalArgumentException("Scan file path is not an absolute URI: " + path);
      }
    } catch (URISyntaxException failure) {
      throw new IllegalArgumentException("Scan file path is not a valid URI: " + path, failure);
    }
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public Row getFileConstants() {
    return fileConstants;
  }

  public Optional<DeletionVectorDescriptor> getDeletionVector() {
    return deletionVector;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ScanFile)) {
      return false;
    }
    ScanFile that = (ScanFile) other;
    return fileStatus.equals(that.fileStatus)
        && RowKernels.equal(fileConstants, that.fileConstants)
        && deletionVector.equals(that.deletionVector);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileStatus, RowKernels.hash(fileConstants), deletionVector);
  }
}
