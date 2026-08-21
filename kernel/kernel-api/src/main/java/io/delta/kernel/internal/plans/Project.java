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

import io.delta.kernel.expressions.Expression;
import io.delta.kernel.types.StructType;
import java.util.List;

/** Projects one input through an expression into a declared output schema. */
public final class Project implements Operator {
  private final Expression expression;
  private final StructType schema;

  public Project(Expression expression, StructType schema) {
    this.expression = requireNonNull(expression, "expression is null");
    this.schema = requireNonNull(schema, "schema is null");
  }

  public Expression getExpression() {
    return expression;
  }

  public StructType getSchema() {
    return schema;
  }

  @Override
  public StructType getOutputSchema(List<StructType> inputSchemas) {
    PlanValidation.requireUnaryInput(inputSchemas, "Project");
    return schema;
  }

  @Override
  public <T> T accept(OperatorVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
