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
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

/** Validation shared by declarative plan nodes. */
final class PlanValidation {
  private PlanValidation() {}

  static void requireRow(Row row, StructType schema, String context) {
    requireNonNull(row, "row is null");
    if (!schema.equals(requireNonNull(row.getSchema(), "row schema is null"))) {
      throw new IllegalArgumentException(context + " schema differs from " + schema);
    }
    for (int ordinal = 0; ordinal < schema.length(); ordinal++) {
      StructField field = schema.at(ordinal);
      if (!field.isNullable() && row.isNullAt(ordinal)) {
        throw new IllegalArgumentException(
            context + " has null for non-nullable field `" + field.getName() + "`");
      }
    }
  }
}
