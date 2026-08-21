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

import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/** Fluent builder for an immutable {@link Plan}. */
public final class PlanBuilder {
  private final BuilderNode root;

  private PlanBuilder(Operator operator, List<BuilderNode> inputs) {
    List<StructType> inputSchemas = new ArrayList<>(inputs.size());
    for (BuilderNode input : inputs) {
      inputSchemas.add(input.outputSchema);
    }
    StructType outputSchema =
        requireNonNull(
            operator.getOutputSchema(Collections.unmodifiableList(inputSchemas)),
            "operator output schema is null");
    this.root =
        new BuilderNode(
            operator, Collections.unmodifiableList(new ArrayList<>(inputs)), outputSchema);
  }

  /** An inline row source. Empty rows remain a present, runnable source. */
  public static PlanBuilder values(StructType schema, List<? extends Row> rows) {
    return source(new Values(schema, rows));
  }

  /** Unordered bag union of one or more builders with the same output schema. */
  public static PlanBuilder unionAll(List<PlanBuilder> inputs) {
    requireNonNull(inputs, "inputs is null");
    List<BuilderNode> roots = new ArrayList<>(inputs.size());
    for (PlanBuilder input : inputs) {
      roots.add(requireNonNull(input, "input builder is null").root);
    }
    if (inputs.size() == 1) {
      return inputs.get(0);
    }
    return new PlanBuilder(UnionAll.UNION_ALL, roots);
  }

  /** Builds a topologically ordered plan containing the nodes reachable from this builder. */
  public Plan build() {
    List<PlanNode> nodes = new ArrayList<>();
    emit(root, nodes, new IdentityHashMap<>());
    return new Plan(nodes);
  }

  private static PlanBuilder source(Operator operator) {
    return new PlanBuilder(operator, Collections.emptyList());
  }

  private static int emit(
      BuilderNode node, List<PlanNode> nodes, Map<BuilderNode, Integer> emitted) {
    Integer existing = emitted.get(node);
    if (existing != null) {
      return existing;
    }

    List<Integer> inputs = new ArrayList<>(node.inputs.size());
    for (BuilderNode input : node.inputs) {
      inputs.add(emit(input, nodes, emitted));
    }
    int nodeIndex = nodes.size();
    nodes.add(new PlanNode(node.operator, inputs));
    emitted.put(node, nodeIndex);
    return nodeIndex;
  }

  private static final class BuilderNode {
    private final Operator operator;
    private final List<BuilderNode> inputs;
    private final StructType outputSchema;

    private BuilderNode(Operator operator, List<BuilderNode> inputs, StructType outputSchema) {
      this.operator = operator;
      this.inputs = inputs;
      this.outputSchema = outputSchema;
    }
  }
}
