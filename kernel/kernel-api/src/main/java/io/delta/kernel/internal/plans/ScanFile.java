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
package io.delta.kernel.internal.plans;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;
import java.util.Collections;

/** One file to scan and the constant values broadcast over rows read from that file. */
public final class ScanFile {
  private static final Row NO_CONSTANTS =
      GenericRow.fromValues(new StructType(), Collections.emptyList());

  private final FileStatus fileStatus;
  private final Row fileConstants;

  public ScanFile(FileStatus fileStatus) {
    this(fileStatus, NO_CONSTANTS);
  }

  public ScanFile(FileStatus fileStatus, Row fileConstants) {
    this.fileStatus = requireNonNull(fileStatus, "fileStatus is null");
    this.fileConstants = requireNonNull(fileConstants, "fileConstants is null");
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public Row getFileConstants() {
    return fileConstants;
  }
}
