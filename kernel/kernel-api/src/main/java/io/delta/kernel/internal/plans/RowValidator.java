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

/** Construction-time validation shared by plan operators that carry inline rows. */
final class RowValidator {
  private RowValidator() {}

  static void validate(Row row, StructType expectedSchema, String context) {
    requireNonNull(row, "row is null");
    requireNonNull(expectedSchema, "expectedSchema is null");
    StructType rowSchema = requireNonNull(row.getSchema(), "row schema is null");
    if (!expectedSchema.equals(rowSchema)) {
      throw new IllegalArgumentException(
          String.format(
              "%s schema differs: has %s value(s) with schema %s, expected %s with schema %s",
              context, rowSchema.length(), rowSchema, expectedSchema.length(), expectedSchema));
    }
    for (int fieldIndex = 0; fieldIndex < expectedSchema.length(); fieldIndex++) {
      StructField field = expectedSchema.at(fieldIndex);
      if (!field.isNullable() && row.isNullAt(fieldIndex)) {
        throw new IllegalArgumentException(
            String.format("%s has null for non-nullable field `%s`", context, field.getName()));
      }
    }
  }
}
