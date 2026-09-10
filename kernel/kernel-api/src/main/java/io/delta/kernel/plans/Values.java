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
import io.delta.kernel.internal.util.RowKernels;
import io.delta.kernel.types.StructType;
import java.util.Collections;
import java.util.List;

/** Inline owned rows matching a declared output schema. */
public final class Values extends PlanNode {
  private final StructType schema;
  private final List<Row> ownedRows;

  public Values(StructType schema, List<? extends Row> ownedRows) {
    this.schema = requireNonNull(schema, "schema is null");
    this.ownedRows = PlanValidation.immutableCopy(ownedRows, "row is null");
    for (int index = 0; index < this.ownedRows.size(); index++) {
      PlanValidation.requireRow(this.ownedRows.get(index), schema, "Values row " + index);
    }
  }

  public List<Row> ownedRows() {
    return ownedRows;
  }

  @Override
  public StructType outputSchema() {
    return schema;
  }

  @Override
  public List<PlanNode> children() {
    return Collections.emptyList();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Values)) {
      return false;
    }
    Values that = (Values) other;
    if (!schema.equals(that.schema) || ownedRows.size() != that.ownedRows.size()) {
      return false;
    }
    for (int index = 0; index < ownedRows.size(); index++) {
      if (!RowKernels.equal(ownedRows.get(index), that.ownedRows.get(index))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    if (hasMemoizedHashCode()) return memoizedHashCode();
    int result = 31 * Values.class.hashCode() + schema.hashCode();
    for (Row row : ownedRows) {
      result = 31 * result + RowKernels.hash(row);
    }
    return memoizeHashCode(result);
  }
}
