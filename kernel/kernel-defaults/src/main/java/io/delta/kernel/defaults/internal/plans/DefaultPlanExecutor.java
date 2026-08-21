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
package io.delta.kernel.defaults.internal.plans;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.plans.Aggregate;
import io.delta.kernel.internal.plans.Filter;
import io.delta.kernel.internal.plans.OperatorVisitor;
import io.delta.kernel.internal.plans.Plan;
import io.delta.kernel.internal.plans.PlanNode;
import io.delta.kernel.internal.plans.Project;
import io.delta.kernel.internal.plans.ScanJson;
import io.delta.kernel.internal.plans.ScanParquet;
import io.delta.kernel.internal.plans.SemiJoin;
import io.delta.kernel.internal.plans.UnionAll;
import io.delta.kernel.internal.plans.Values;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Executes typed plans using Kernel Java batches and engine handlers. */
public final class DefaultPlanExecutor
    implements OperatorVisitor<DefaultPlanExecutor.BatchOperator> {
  private final Plan plan;
  private final Engine engine;
  private final ExecutionNode[] compiled;
  private final int[] fanout;

  private DefaultPlanExecutor(Plan plan, Engine engine) {
    this.plan = requireNonNull(plan, "plan is null");
    this.engine = requireNonNull(engine, "engine is null");
    this.compiled = new ExecutionNode[plan.getNodes().size()];
    this.fanout = countFanout(plan);
  }

  /** Executes the terminal node of {@code plan}. */
  public static CloseableIterator<FilteredColumnarBatch> execute(Plan plan, Engine engine) {
    DefaultPlanExecutor executor = new DefaultPlanExecutor(plan, engine);
    return executor.compile(plan.getNodes().size() - 1).open();
  }

  @Override
  public BatchOperator visit(Aggregate aggregate) {
    return (inputs, schemas) -> AggregateExecutor.execute(aggregate, schemas.get(0), inputs.get(0));
  }

  @Override
  public BatchOperator visit(Filter filter) {
    return (inputs, schemas) -> FilterExecutor.execute(filter, schemas.get(0), inputs.get(0));
  }

  @Override
  public BatchOperator visit(Project project) {
    return (inputs, schemas) -> ProjectExecutor.execute(project, schemas.get(0), inputs.get(0));
  }

  @Override
  public BatchOperator visit(ScanParquet scan) {
    return (inputs, schemas) -> FileScanExecutor.execute(scan, engine);
  }

  @Override
  public BatchOperator visit(ScanJson scan) {
    return (inputs, schemas) -> FileScanExecutor.execute(scan, engine);
  }

  @Override
  public BatchOperator visit(SemiJoin join) {
    return (inputs, schemas) ->
        SemiJoinExecutor.execute(
            join, schemas.get(0), schemas.get(1), inputs.get(0), inputs.get(1));
  }

  @Override
  public BatchOperator visit(Values values) {
    return (inputs, schemas) -> ValuesExecutor.execute(values);
  }

  @Override
  public BatchOperator visit(UnionAll union) {
    return (inputs, schemas) -> UnionAllExecutor.execute(inputs);
  }

  private ExecutionNode compile(int nodeIndex) {
    ExecutionNode existing = compiled[nodeIndex];
    if (existing != null) {
      return existing;
    }

    PlanNode node = plan.getNodes().get(nodeIndex);
    List<ExecutionNode> inputs = new ArrayList<>(node.getInputs().size());
    for (int inputIndex : node.getInputs()) {
      inputs.add(compile(inputIndex));
    }
    List<StructType> inputSchemas = new ArrayList<>(node.getInputs().size());
    for (int inputIndex : node.getInputs()) {
      inputSchemas.add(plan.getOutputSchema(inputIndex));
    }
    ExecutionNode execution =
        new ExecutionNode(
            nodeIndex,
            inputs,
            inputSchemas,
            node.getOperator().accept(this),
            fanout[nodeIndex] > 1);
    compiled[nodeIndex] = execution;
    return execution;
  }

  private static int[] countFanout(Plan plan) {
    boolean[] reachable = new boolean[plan.getNodes().size()];
    int[] fanout = new int[plan.getNodes().size()];
    reachable[reachable.length - 1] = true;
    for (int nodeIndex = reachable.length - 1; nodeIndex >= 0; nodeIndex--) {
      if (reachable[nodeIndex]) {
        for (int inputIndex : plan.getNodes().get(nodeIndex).getInputs()) {
          fanout[inputIndex] = Math.addExact(fanout[inputIndex], 1);
          reachable[inputIndex] = true;
        }
      }
    }
    return fanout;
  }

  @FunctionalInterface
  interface BatchOperator {
    CloseableIterator<FilteredColumnarBatch> open(
        List<CloseableIterator<FilteredColumnarBatch>> inputs, List<StructType> inputSchemas);
  }

  private static final class ExecutionNode {
    private final int nodeIndex;
    private final List<ExecutionNode> inputs;
    private final List<StructType> inputSchemas;
    private final BatchOperator operator;
    private final boolean replayable;
    private boolean opened;
    private List<FilteredColumnarBatch> materialized;

    private ExecutionNode(
        int nodeIndex,
        List<ExecutionNode> inputs,
        List<StructType> inputSchemas,
        BatchOperator operator,
        boolean replayable) {
      this.nodeIndex = nodeIndex;
      this.inputs = Collections.unmodifiableList(new ArrayList<>(inputs));
      this.inputSchemas = Collections.unmodifiableList(new ArrayList<>(inputSchemas));
      this.operator = requireNonNull(operator, "operator is null");
      this.replayable = replayable;
    }

    private CloseableIterator<FilteredColumnarBatch> open() {
      if (!replayable) {
        return openOnce();
      }
      if (materialized == null) {
        materialized = openOnce().toInMemoryList();
      }
      return toCloseableIterator(materialized.iterator());
    }

    private CloseableIterator<FilteredColumnarBatch> openOnce() {
      if (opened) {
        throw new IllegalStateException("Plan node " + nodeIndex + " was opened more than once");
      }
      opened = true;
      List<CloseableIterator<FilteredColumnarBatch>> streams = new ArrayList<>(inputs.size());
      try {
        for (ExecutionNode input : inputs) {
          streams.add(input.open());
        }
        return operator.open(streams, inputSchemas);
      } catch (RuntimeException | Error failure) {
        for (AutoCloseable stream : streams) {
          io.delta.kernel.internal.util.Utils.closeCloseablesAndAddSuppressed(failure, stream);
        }
        throw failure;
      }
    }
  }
}
