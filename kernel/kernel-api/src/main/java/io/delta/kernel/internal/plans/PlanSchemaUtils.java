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
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

/** Schema operations shared by plan IR validation. */
final class PlanSchemaUtils {
  private PlanSchemaUtils() {}

  static StructField resolveField(StructType schema, Column column, String context) {
    requireNonNull(schema, "schema is null");
    requireNonNull(column, "column is null");
    String[] names = requireNonNull(column.getNames(), "column path is null");
    if (names.length == 0) {
      throw unresolved(column, context);
    }

    DataType currentType = schema;
    StructField field = null;
    for (String name : names) {
      requireNonNull(name, "column path part is null");
      if (!(currentType instanceof StructType)) {
        throw unresolved(column, context);
      }
      StructType struct = (StructType) currentType;
      int ordinal = struct.indexOf(name);
      if (ordinal < 0) {
        throw unresolved(column, context);
      }
      field = struct.at(ordinal);
      currentType = field.getDataType();
    }
    return field;
  }

  private static IllegalArgumentException unresolved(Column column, String context) {
    return new IllegalArgumentException(context + " column " + column + " is absent from schema");
  }
}
