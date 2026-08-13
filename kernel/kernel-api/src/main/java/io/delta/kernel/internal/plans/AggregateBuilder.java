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
import java.util.List;

/** Derives an {@link Aggregate} output schema from its input columns. */
public final class AggregateBuilder {
  private final StructType inputSchema;
  private final List<Column> groupBy;
  private final List<Agg> aggs = new ArrayList<>();
  private final List<String> aliases = new ArrayList<>();

  AggregateBuilder(StructType inputSchema, List<Column> groupBy) {
    this.inputSchema = requireNonNull(inputSchema, "inputSchema is null");
    requireNonNull(groupBy, "groupBy is null");
    this.groupBy = new ArrayList<>(groupBy.size());
    for (Column key : groupBy) {
      this.groupBy.add(requireNonNull(key, "group key is null"));
    }
  }

  /** Adds an aggregate using its value column's leaf name as the output name. */
  public AggregateBuilder aggregate(Agg agg) {
    aggs.add(requireNonNull(agg, "agg is null"));
    aliases.add(null);
    return this;
  }

  /** Adds an aggregate with an explicit output name. */
  public AggregateBuilder aggregateAs(Agg agg, String outputName) {
    aggs.add(requireNonNull(agg, "agg is null"));
    aliases.add(requireNonNull(outputName, "outputName is null"));
    return this;
  }

  public AggregateBuilder min(Column value) {
    return aggregate(Agg.min(value));
  }

  public AggregateBuilder max(Column value) {
    return aggregate(Agg.max(value));
  }

  public AggregateBuilder minNonNullBy(Column value, Column key) {
    return aggregate(Agg.minNonNullBy(value, key));
  }

  public AggregateBuilder maxNonNullBy(Column value, Column key) {
    return aggregate(Agg.maxNonNullBy(value, key));
  }

  /** Resolves all input columns and constructs the immutable aggregate payload. */
  public Aggregate build() {
    List<StructField> fields = new ArrayList<>(groupBy.size() + aggs.size());
    for (Column key : groupBy) {
      fields.add(PlanSchemaUtils.resolveField(inputSchema, key, "Aggregate group key"));
    }
    for (int index = 0; index < aggs.size(); index++) {
      fields.add(aggs.get(index).outputField(inputSchema, aliases.get(index)));
    }
    Aggregate.validateUniqueNames(fields);
    return new Aggregate(groupBy, aggs, new StructType(fields));
  }
}
