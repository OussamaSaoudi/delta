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
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

  static StructType requireUnaryInput(List<StructType> inputSchemas, String operator) {
    requireNonNull(inputSchemas, "inputSchemas is null");
    if (inputSchemas.size() != 1) {
      throw new IllegalArgumentException(
          operator + " requires one input, got " + inputSchemas.size());
    }
    return requireNonNull(inputSchemas.get(0), "input schema is null");
  }

  static <T> List<T> immutableCopy(List<? extends T> values, String nullMessage) {
    requireNonNull(values, "values is null");
    List<T> copy = new ArrayList<>(values.size());
    for (T value : values) {
      copy.add(requireNonNull(value, nullMessage));
    }
    return Collections.unmodifiableList(copy);
  }

  static StructField resolveField(StructType schema, Column column, String context) {
    requireNonNull(schema, "schema is null");
    requireNonNull(column, "column is null");
    DataType current = schema;
    StructField field = null;
    for (String name : column.getNames()) {
      if (!(current instanceof StructType)) {
        throw unresolved(column, context);
      }
      int ordinal = ((StructType) current).indexOf(requireNonNull(name, "column name is null"));
      if (ordinal < 0) {
        throw unresolved(column, context);
      }
      field = ((StructType) current).at(ordinal);
      current = field.getDataType();
    }
    if (field == null) {
      throw unresolved(column, context);
    }
    return field;
  }

  static void validateExpressionReferences(
      StructType schema, Expression expression, String context) {
    requireNonNull(expression, "expression is null");
    if (expression instanceof Column) {
      resolveField(schema, (Column) expression, context);
    }
    for (Expression child : expression.getChildren()) {
      validateExpressionReferences(schema, child, context);
    }
  }

  static DataType stripFieldMetadata(DataType type) {
    if (type instanceof StructType) {
      List<StructField> fields = new ArrayList<>(((StructType) type).length());
      for (StructField field : ((StructType) type).fields()) {
        fields.add(stripFieldMetadata(field));
      }
      return new StructType(fields);
    }
    if (type instanceof ArrayType) {
      return new ArrayType(stripFieldMetadata(((ArrayType) type).getElementField()));
    }
    if (type instanceof MapType) {
      MapType map = (MapType) type;
      return new MapType(
          stripFieldMetadata(map.getKeyField()), stripFieldMetadata(map.getValueField()));
    }
    return type;
  }

  private static StructField stripFieldMetadata(StructField field) {
    return new StructField(
        field.getName(), stripFieldMetadata(field.getDataType()), field.isNullable());
  }

  private static IllegalArgumentException unresolved(Column column, String context) {
    return new IllegalArgumentException(context + " column " + column + " is absent from schema");
  }
}
