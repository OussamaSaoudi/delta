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

import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import java.util.List;

/** Keeps input rows for which {@code predicate} is true. */
public final class Filter implements Operator {
  private final Predicate predicate;

  public Filter(Predicate predicate) {
    this.predicate = requireNonNull(predicate, "predicate is null");
  }

  public Predicate getPredicate() {
    return predicate;
  }

  @Override
  public StructType getOutputSchema(List<StructType> inputSchemas) {
    StructType input = PlanValidation.requireUnaryInput(inputSchemas, "Filter");
    PlanValidation.validateExpressionReferences(input, predicate, "Filter predicate");
    return input;
  }

  @Override
  public <T> T accept(OperatorVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
