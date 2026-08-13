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
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Groups input rows and computes aggregate columns. */
public final class Aggregate implements Operator {
  private final List<Column> groupBy;
  private final List<Agg> aggs;
  private final StructType schema;

  Aggregate(List<Column> groupBy, List<Agg> aggs, StructType schema) {
    this.groupBy = immutableCopy(groupBy, "group key is null");
    this.aggs = immutableCopy(aggs, "agg is null");
    this.schema = requireNonNull(schema, "schema is null");
  }

  /** Starts a global aggregate over {@code inputSchema}. */
  public static AggregateBuilder ungrouped(StructType inputSchema) {
    return groupBy(inputSchema, Collections.emptyList());
  }

  /** Starts an aggregate grouped by {@code keys}. */
  public static AggregateBuilder groupBy(StructType inputSchema, List<Column> keys) {
    return new AggregateBuilder(inputSchema, keys);
  }

  private static <T> List<T> immutableCopy(List<T> values, String nullMessage) {
    requireNonNull(values, "values is null");
    List<T> copy = new ArrayList<>(values.size());
    for (T value : values) {
      copy.add(requireNonNull(value, nullMessage));
    }
    return Collections.unmodifiableList(copy);
  }

  static void validateUniqueNames(List<StructField> fields) {
    Set<String> names = new HashSet<>();
    for (StructField field : fields) {
      String name = requireNonNull(field.getName(), "output field name is null");
      if (!names.add(name.toLowerCase(Locale.ROOT))) {
        throw new IllegalArgumentException("Duplicate aggregate output name `" + name + "`");
      }
    }
  }

  @Override
  public StructType getOutputSchema(List<StructType> inputSchemas) {
    requireNonNull(inputSchemas, "inputSchemas is null");
    if (inputSchemas.size() != 1) {
      throw new IllegalArgumentException(
          "Aggregate requires one input, got " + inputSchemas.size());
    }
    StructType input = requireNonNull(inputSchemas.get(0), "input schema is null");
    List<StructField> actualFields = new ArrayList<>(schema.length());
    for (Column key : groupBy) {
      actualFields.add(PlanSchemaUtils.resolveField(input, key, "Aggregate group key"));
    }
    for (int index = 0; index < aggs.size(); index++) {
      String outputName = schema.at(groupBy.size() + index).getName();
      actualFields.add(aggs.get(index).outputField(input, outputName));
    }
    StructType actualSchema = new StructType(actualFields);
    if (!schema.equals(actualSchema)) {
      throw new IllegalArgumentException(
          "Aggregate output schema does not match its input: expected "
              + schema
              + ", got "
              + actualSchema);
    }
    return schema;
  }

  public List<Column> getGroupBy() {
    return groupBy;
  }

  public List<Agg> getAggs() {
    return aggs;
  }

  public StructType getSchema() {
    return schema;
  }
}
