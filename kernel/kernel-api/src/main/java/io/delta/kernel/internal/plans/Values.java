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
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Inline rows matching a declared output schema. */
public final class Values implements Operator {
  private final StructType schema;
  private final List<Row> rows;

  public Values(StructType schema, List<? extends Row> rows) {
    this.schema = requireNonNull(schema, "schema is null");
    requireNonNull(rows, "rows is null");

    List<Row> copiedRows = new ArrayList<>(rows.size());
    for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
      Row row = requireNonNull(rows.get(rowIndex), "row is null");
      RowValidator.validate(row, schema, String.format("Values row %s", rowIndex));
      copiedRows.add(row);
    }
    this.rows = Collections.unmodifiableList(copiedRows);
  }

  public StructType getSchema() {
    return schema;
  }

  public List<Row> getRows() {
    return rows;
  }

  @Override
  public StructType getOutputSchema(List<StructType> inputSchemas) {
    requireNonNull(inputSchemas, "inputSchemas is null");
    if (!inputSchemas.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Values requires no inputs, got %s", inputSchemas.size()));
    }
    return schema;
  }
}
