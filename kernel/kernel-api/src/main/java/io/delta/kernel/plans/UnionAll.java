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

import io.delta.kernel.types.StructType;
import java.util.List;

/** Unordered bag union whose inputs have one exact schema. */
public final class UnionAll extends PlanNode {
  private final List<PlanNode> inputs;
  private final StructType outputSchema;

  public UnionAll(List<PlanNode> inputs) {
    this.inputs = PlanValidation.immutableCopy(inputs, "union input is null");
    if (this.inputs.isEmpty()) {
      throw new IllegalArgumentException("UnionAll requires at least one input");
    }
    this.outputSchema = this.inputs.get(0).outputSchema();
    for (int index = 1; index < this.inputs.size(); index++) {
      StructType schema = this.inputs.get(index).outputSchema();
      if (!outputSchema.equals(schema)) {
        throw new IllegalArgumentException(
            "UnionAll input " + index + " schema differs from input 0: " + schema);
      }
    }
  }

  public List<PlanNode> inputs() {
    return inputs;
  }

  @Override
  public StructType outputSchema() {
    return outputSchema;
  }

  @Override
  public List<PlanNode> children() {
    return inputs;
  }

  @Override
  public boolean equals(Object other) {
    return this == other || (other instanceof UnionAll && inputs.equals(((UnionAll) other).inputs));
  }

  @Override
  public int hashCode() {
    if (hasMemoizedHashCode()) return memoizedHashCode();
    return memoizeHashCode(31 * UnionAll.class.hashCode() + inputs.hashCode());
  }
}
