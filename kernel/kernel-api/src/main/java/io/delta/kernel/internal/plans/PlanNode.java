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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** An operator and the indices of the upstream plan nodes it consumes. */
public final class PlanNode {
  private final Operator operator;
  private final List<Integer> inputs;

  public PlanNode(Operator operator, List<Integer> inputs) {
    this.operator = requireNonNull(operator, "operator is null");
    requireNonNull(inputs, "inputs is null");

    List<Integer> copiedInputs = new ArrayList<>(inputs.size());
    for (int position = 0; position < inputs.size(); position++) {
      Integer input = requireNonNull(inputs.get(position), "input index is null");
      if (input < 0) {
        throw new IllegalArgumentException(
            String.format("Input %s has a negative node index: %s", position, input));
      }
      copiedInputs.add(input);
    }
    this.inputs = Collections.unmodifiableList(copiedInputs);
  }

  public Operator getOperator() {
    return operator;
  }

  public List<Integer> getInputs() {
    return inputs;
  }
}
