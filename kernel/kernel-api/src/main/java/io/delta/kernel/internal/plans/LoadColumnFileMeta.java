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

import io.delta.kernel.expressions.Column;

/** Columns read from each upstream row to locate and size one file for {@link Load}. */
public final class LoadColumnFileMeta {
  private final Column pathColumn;
  private final Column fileSizeColumn;
  private final Column numRecordsColumn;

  public LoadColumnFileMeta(Column pathColumn, Column fileSizeColumn, Column numRecordsColumn) {
    this.pathColumn = requireNonNull(pathColumn, "pathColumn is null");
    this.fileSizeColumn = requireNonNull(fileSizeColumn, "fileSizeColumn is null");
    this.numRecordsColumn = requireNonNull(numRecordsColumn, "numRecordsColumn is null");
  }

  public Column getPathColumn() {
    return pathColumn;
  }

  public Column getFileSizeColumn() {
    return fileSizeColumn;
  }

  public Column getNumRecordsColumn() {
    return numRecordsColumn;
  }
}
