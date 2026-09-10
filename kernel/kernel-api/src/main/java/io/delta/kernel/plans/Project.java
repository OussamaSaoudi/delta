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
import io.delta.kernel.types.StructType;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Projects one input through one struct-valued expression. */
public final class Project extends PlanNode {
  private final PlanNode input;
  private final Expression rowExpression;
  private final StructType outputSchema;
  private final List<PlanNode> children;

  public Project(PlanNode input, Expression rowExpression, StructType outputSchema) {
    this.input = requireNonNull(input, "input is null");
    this.rowExpression = requireNonNull(rowExpression, "rowExpression is null");
    this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
    this.children = Collections.singletonList(input);
    PlanValidation.validateExpressionReferences(
        input.outputSchema(), rowExpression, "Project expression");
  }

  public PlanNode input() {
    return input;
  }

  public Expression rowExpression() {
    return rowExpression;
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
    if (!(other instanceof Project)) {
      return false;
    }
    Project that = (Project) other;
    return input.equals(that.input)
        && rowExpression.equals(that.rowExpression)
        && outputSchema.equals(that.outputSchema);
  }

  @Override
  public int hashCode() {
    if (hasMemoizedHashCode()) return memoizedHashCode();
    return memoizeHashCode(Objects.hash(input, rowExpression, outputSchema));
  }
}
