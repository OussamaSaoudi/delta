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
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.List;

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

  /** Returns an equivalent type with field metadata removed at every nesting level. */
  static DataType stripFieldMetadata(DataType dataType) {
    requireNonNull(dataType, "dataType is null");
    if (dataType instanceof StructType) {
      StructType struct = (StructType) dataType;
      List<StructField> fields = new ArrayList<>(struct.length());
      for (StructField field : struct.fields()) {
        fields.add(stripFieldMetadata(field));
      }
      return new StructType(fields);
    }
    if (dataType instanceof ArrayType) {
      ArrayType array = (ArrayType) dataType;
      return new ArrayType(stripFieldMetadata(array.getElementField()));
    }
    if (dataType instanceof MapType) {
      MapType map = (MapType) dataType;
      return new MapType(
          stripFieldMetadata(map.getKeyField()), stripFieldMetadata(map.getValueField()));
    }
    return dataType;
  }

  private static StructField stripFieldMetadata(StructField field) {
    return new StructField(
        field.getName(), stripFieldMetadata(field.getDataType()), field.isNullable());
  }

  private static IllegalArgumentException unresolved(Column column, String context) {
    return new IllegalArgumentException(context + " column " + column + " is absent from schema");
  }
}
