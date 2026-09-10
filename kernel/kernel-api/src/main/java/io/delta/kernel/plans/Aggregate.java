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

import io.delta.kernel.expressions.Expression;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Groups input rows and computes aggregate columns. */
public final class Aggregate extends PlanNode {
  private final PlanNode input;
  private final List<Expression> groupBy;
  private final List<Agg> aggregates;
  private final StructType outputSchema;
  private final List<PlanNode> children;

  public Aggregate(
      PlanNode input, List<Expression> groupBy, List<Agg> aggregates, StructType outputSchema) {
    this.input = requireNonNull(input, "input is null");
    this.groupBy = PlanValidation.immutableCopy(groupBy, "grouping expression is null");
    this.aggregates = PlanValidation.immutableCopy(aggregates, "aggregate is null");
    this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
    this.children = Collections.singletonList(input);

    int expectedFields = this.groupBy.size() + this.aggregates.size();
    if (outputSchema.length() != expectedFields) {
      throw new IllegalArgumentException(
          "Aggregate output has " + outputSchema.length() + " fields, expected " + expectedFields);
    }
    for (Expression expression : this.groupBy) {
      PlanValidation.validateExpressionReferences(
          input.outputSchema(), expression, "Aggregate grouping expression");
    }
    for (int index = 0; index < this.aggregates.size(); index++) {
      Agg aggregate = this.aggregates.get(index);
      aggregate.validateReferences(input.outputSchema());
      StructField field = outputSchema.at(this.groupBy.size() + index);
      if (!aggregate.outputType().equals(field.getDataType())
          || aggregate.outputNullable() != field.isNullable()) {
        throw new IllegalArgumentException(
            "Aggregate output field `"
                + field.getName()
                + "` must have type "
                + aggregate.outputType()
                + " and nullable="
                + aggregate.outputNullable());
      }
    }
  }

  public PlanNode input() {
    return input;
  }

  public List<Expression> groupBy() {
    return groupBy;
  }

  public List<Agg> aggregates() {
    return aggregates;
  }

  @Override
  public StructType outputSchema() {
    return outputSchema;
  }

  @Override
  public List<PlanNode> children() {
    return children;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Aggregate)) {
      return false;
    }
    Aggregate that = (Aggregate) other;
    return input.equals(that.input)
        && groupBy.equals(that.groupBy)
        && aggregates.equals(that.aggregates)
        && outputSchema.equals(that.outputSchema);
  }

  @Override
  public int hashCode() {
    if (hasMemoizedHashCode()) return memoizedHashCode();
    return memoizeHashCode(Objects.hash(input, groupBy, aggregates, outputSchema));
  }
}
