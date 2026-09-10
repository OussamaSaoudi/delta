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

import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Keeps input rows for which {@code predicate} is true. */
public final class Filter extends PlanNode {
  private final PlanNode input;
  private final Predicate predicate;
  private final List<PlanNode> children;

  public Filter(PlanNode input, Predicate predicate) {
    this.input = requireNonNull(input, "input is null");
    this.predicate = requireNonNull(predicate, "predicate is null");
    this.children = Collections.singletonList(input);
    PlanValidation.validateExpressionReferences(
        input.outputSchema(), predicate, "Filter predicate");
  }

  public PlanNode input() {
    return input;
  }

  public Predicate predicate() {
    return predicate;
  }

  @Override
  public StructType outputSchema() {
    return input.outputSchema();
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
    if (!(other instanceof Filter)) {
      return false;
    }
    Filter that = (Filter) other;
    return input.equals(that.input) && predicate.equals(that.predicate);
  }

  @Override
  public int hashCode() {
    if (hasMemoizedHashCode()) return memoizedHashCode();
    return memoizeHashCode(Objects.hash(input, predicate));
  }
}
