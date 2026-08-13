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
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Fluent builder for a declarative {@link Plan}.
 *
 * <p>Like the Rust plan builder, source factories return a builder rooted at that source and each
 * transform returns a new builder. Builders are immutable and may be reused to form shared plan
 * subgraphs. Empty sources remain ordinary plan nodes; this builder has no absent-relation state.
 */
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

  /** A Parquet scan source. */
  public static PlanBuilder scanParquet(
      List<ScanFile> files, List<String> fileConstantColumns, StructType schema) {
    return source(new ScanParquet(files, fileConstantColumns, schema));
  }

  /** A newline-delimited JSON scan source. */
  public static PlanBuilder scanJson(
      List<ScanFile> files, List<String> fileConstantColumns, StructType schema) {
    return source(new ScanJson(files, fileConstantColumns, schema));
  }

  /** An inline row source. Empty rows remain a present, runnable source. */
  public static PlanBuilder values(StructType schema, List<? extends Row> rows) {
    return source(new Values(schema, rows));
  }

  /** Keeps rows for which {@code predicate} evaluates to true. */
  public PlanBuilder filter(Predicate predicate) {
    return unary(new Filter(predicate));
  }

  /** Projects rows through a struct expression into {@code schema}. */
  public PlanBuilder project(Expression expression, StructType schema) {
    return unary(new Project(expression, schema));
  }

  /** Reads the files described by this builder's rows. */
  public PlanBuilder load(Load load) {
    return unary(requireNonNull(load, "load is null"));
  }

  /** Applies a grouped or global aggregate. */
  public PlanBuilder aggregate(Aggregate aggregate) {
    return unary(requireNonNull(aggregate, "aggregate is null"));
  }

  /** Applies an aggregate grouped by {@code keys}, inferring this builder's output schema. */
  public PlanBuilder aggregateBy(List<Column> keys, UnaryOperator<AggregateBuilder> aggregates) {
    AggregateBuilder builder = Aggregate.groupBy(root.outputSchema, keys);
    return aggregate(
        requireNonNull(
                requireNonNull(aggregates, "aggregates is null").apply(builder),
                "aggregates returned null")
            .build());
  }

  /** Applies an ungrouped aggregate, inferring this builder's output schema. */
  public PlanBuilder aggregateUngrouped(UnaryOperator<AggregateBuilder> aggregates) {
    AggregateBuilder builder = Aggregate.ungrouped(root.outputSchema);
    return aggregate(
        requireNonNull(
                requireNonNull(aggregates, "aggregates is null").apply(builder),
                "aggregates returned null")
            .build());
  }

  /** Keeps rows whose probe keys occur in {@code build}. */
  public PlanBuilder semiJoin(PlanBuilder build, List<Column> probeKeys, List<Column> buildKeys) {
    return join(build, new SemiJoin(false, probeKeys, buildKeys));
  }

  /** Keeps rows whose probe keys do not occur in {@code build}. */
  public PlanBuilder antiJoin(PlanBuilder build, List<Column> probeKeys, List<Column> buildKeys) {
    return join(build, new SemiJoin(true, probeKeys, buildKeys));
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

  /** Linearizes this builder's reachable DAG into a topologically ordered plan. */
  public Plan build() {
    List<PlanNode> nodes = new ArrayList<>();
    emit(root, nodes, new IdentityHashMap<>());
    return new Plan(nodes);
  }

  private static PlanBuilder source(Operator operator) {
    return new PlanBuilder(operator, Collections.emptyList());
  }

  private PlanBuilder unary(Operator operator) {
    return new PlanBuilder(operator, Collections.singletonList(root));
  }

  private PlanBuilder join(PlanBuilder build, SemiJoin join) {
    requireNonNull(build, "build is null");
    return new PlanBuilder(join, Arrays.asList(root, build.root));
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
