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

import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Reads Parquet files with an optional advisory pushdown predicate. */
public final class ScanParquet extends FileScan {
  private final Optional<Predicate> pushdownHint;

  public ScanParquet(
      List<ScanFile> files,
      Optional<URI> tableRoot,
      List<String> fileConstantColumns,
      StructType outputSchema,
      Optional<Predicate> pushdownHint) {
    super(files, tableRoot, fileConstantColumns, outputSchema);
    this.pushdownHint = requireNonNull(pushdownHint, "pushdownHint is null");
    pushdownHint.ifPresent(
        predicate ->
            PlanValidation.validateExpressionReferences(
                outputSchema, predicate, "Parquet pushdown predicate"));
  }

  public Optional<Predicate> pushdownHint() {
    return pushdownHint;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ScanParquet)) {
      return false;
    }
    ScanParquet that = (ScanParquet) other;
    return files().equals(that.files())
        && tableRoot().equals(that.tableRoot())
        && fileConstantColumns().equals(that.fileConstantColumns())
        && outputSchema().equals(that.outputSchema())
        && pushdownHint.equals(that.pushdownHint);
  }

  @Override
  public int hashCode() {
    if (hasMemoizedHashCode()) return memoizedHashCode();
    return memoizeHashCode(
        Objects.hash(
            ScanParquet.class,
            files(),
            tableRoot(),
            fileConstantColumns(),
            outputSchema(),
            pushdownHint));
  }
}
