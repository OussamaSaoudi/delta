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

import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An immutable dataflow DAG in topological order.
 *
 * <p>Every input index must be strictly less than the index of the node that consumes it. The last
 * node is the terminal node whose rows are the result of the plan.
 */
public final class Plan {
  private final List<PlanNode> nodes;
  private final List<StructType> outputSchemas;

  public Plan(List<PlanNode> nodes) {
    requireNonNull(nodes, "nodes is null");
    if (nodes.isEmpty()) {
      throw new IllegalArgumentException("Plan requires at least one node");
    }

    List<PlanNode> copiedNodes = new ArrayList<>(nodes.size());
    List<StructType> schemas = new ArrayList<>(nodes.size());
    for (int nodeIndex = 0; nodeIndex < nodes.size(); nodeIndex++) {
      PlanNode node = requireNonNull(nodes.get(nodeIndex), "plan node is null");
      List<StructType> inputSchemas = resolveInputSchemas(node, nodeIndex, schemas);
      StructType outputSchema =
          requireNonNull(
              node.getOperator().getOutputSchema(inputSchemas), "operator output schema is null");
      copiedNodes.add(node);
      schemas.add(outputSchema);
    }
    this.nodes = Collections.unmodifiableList(copiedNodes);
    this.outputSchemas = Collections.unmodifiableList(schemas);
  }

  private static List<StructType> resolveInputSchemas(
      PlanNode node, int nodeIndex, List<StructType> priorSchemas) {
    List<StructType> inputSchemas = new ArrayList<>(node.getInputs().size());
    for (int inputPosition = 0; inputPosition < node.getInputs().size(); inputPosition++) {
      int inputIndex = node.getInputs().get(inputPosition);
      if (inputIndex >= nodeIndex) {
        throw new IllegalArgumentException(
            String.format(
                "Plan node %s input %s references node %s; inputs must reference an earlier node",
                nodeIndex, inputPosition, inputIndex));
      }
      inputSchemas.add(priorSchemas.get(inputIndex));
    }
    return Collections.unmodifiableList(inputSchemas);
  }

  public List<PlanNode> getNodes() {
    return nodes;
  }

  public StructType getOutputSchema(int nodeIndex) {
    return outputSchemas.get(nodeIndex);
  }

  public StructType getOutputSchema() {
    return outputSchemas.get(outputSchemas.size() - 1);
  }
}
