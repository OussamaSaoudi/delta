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

import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Reads newline-delimited JSON files. */
public final class ScanJson extends FileScan {
  public ScanJson(
      List<ScanFile> files,
      Optional<URI> tableRoot,
      List<String> fileConstantColumns,
      StructType outputSchema) {
    super(files, tableRoot, fileConstantColumns, outputSchema);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ScanJson)) {
      return false;
    }
    ScanJson that = (ScanJson) other;
    return files().equals(that.files())
        && tableRoot().equals(that.tableRoot())
        && fileConstantColumns().equals(that.fileConstantColumns())
        && outputSchema().equals(that.outputSchema());
  }

  @Override
  public int hashCode() {
    if (hasMemoizedHashCode()) return memoizedHashCode();
    return memoizeHashCode(
        Objects.hash(ScanJson.class, files(), tableRoot(), fileConstantColumns(), outputSchema()));
  }
}
