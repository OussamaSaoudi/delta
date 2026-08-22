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
    this.groupBy = PlanValidation.immutableCopy(groupBy, "grouping column is null");
    this.aggs = PlanValidation.immutableCopy(aggs, "agg is null");
    this.schema = requireNonNull(schema, "schema is null");
  }

  /** Starts a global aggregate over {@code inputSchema}. */
  public static AggregateBuilder ungrouped(StructType inputSchema) {
    return groupBy(inputSchema, java.util.Collections.emptyList());
  }

  /** Starts an aggregate grouped by {@code columns}. */
  public static AggregateBuilder groupBy(StructType inputSchema, List<Column> columns) {
    return new AggregateBuilder(inputSchema, columns);
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
    StructType input = PlanValidation.requireUnaryInput(inputSchemas, "Aggregate");
    List<StructField> actualFields = new ArrayList<>(schema.length());
    for (Column column : groupBy) {
      actualFields.add(PlanValidation.resolveField(input, column, "Aggregate grouping column"));
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

  public List<Agg> getAggs() {
    return aggs;
  }

  public List<Column> getGroupBy() {
    return groupBy;
  }

  public StructType getSchema() {
    return schema;
  }

  @Override
  public <T> T accept(OperatorVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
