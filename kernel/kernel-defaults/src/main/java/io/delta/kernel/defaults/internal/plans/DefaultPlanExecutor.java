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
import io.delta.kernel.internal.plans.Load;
import io.delta.kernel.internal.plans.Operator;
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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Executes typed Kernel plans using Kernel Java batches, expressions, and file handlers. */
public final class DefaultPlanExecutor {
  private DefaultPlanExecutor() {}

  /** Executes {@code plan} with an I/O parallelism chosen for the current JVM. */
  public static CloseableIterator<FilteredColumnarBatch> execute(Plan plan, Engine engine) {
    return execute(plan, engine, defaultIoParallelism());
  }

  /** Executes {@code plan} and reports opt-in per-node execution measurements. */
  public static CloseableIterator<FilteredColumnarBatch> execute(
      Plan plan, Engine engine, PlanExecutionObserver observer) {
    requireNonNull(observer, "observer is null");
    if (observer == PlanExecutionObserver.NOOP) {
      return execute(plan, engine);
    }
    return execute(plan, engine, defaultIoParallelism(), observer);
  }

  /**
   * Executes {@code plan} with up to {@code ioParallelism} concurrent blocking I/O operations.
   *
   * <p>All reachable leaf scans are opened and submitted before pull execution begins. Ordinary
   * single-consumer edges stream through {@link CloseableIterator}; only a node referenced by
   * multiple consumers is materialized for replay.
   */
  public static CloseableIterator<FilteredColumnarBatch> execute(
      Plan plan, Engine engine, int ioParallelism) {
    if (ioParallelism <= 0) {
      throw new IllegalArgumentException("ioParallelism must be positive: " + ioParallelism);
    }
    ExecutorService ioExecutor = Executors.newFixedThreadPool(ioParallelism);
    try {
      return execute(plan, engine, ioExecutor, true);
    } catch (RuntimeException | Error failure) {
      ioExecutor.shutdownNow();
      throw failure;
    }
  }

  /**
   * Executes with explicit I/O parallelism and reports opt-in per-node measurements.
   *
   * <p>Measurements are delivered when the returned iterator is closed or exhausted.
   */
  public static CloseableIterator<FilteredColumnarBatch> execute(
      Plan plan, Engine engine, int ioParallelism, PlanExecutionObserver observer) {
    requireNonNull(observer, "observer is null");
    if (observer == PlanExecutionObserver.NOOP) {
      return execute(plan, engine, ioParallelism);
    }
    if (ioParallelism <= 0) {
      throw new IllegalArgumentException("ioParallelism must be positive: " + ioParallelism);
    }
    ExecutorService ioExecutor = Executors.newFixedThreadPool(ioParallelism);
    try {
      return execute(plan, engine, ioExecutor, true, observer);
    } catch (RuntimeException | Error failure) {
      ioExecutor.shutdownNow();
      throw failure;
    }
  }

  /**
   * Executes with a caller-owned executor.
   *
   * <p>Closing or exhausting the returned iterator cancels this query's outstanding work but does
   * not shut down {@code ioExecutor}. The caller remains responsible for the executor's lifecycle.
   */
  public static CloseableIterator<FilteredColumnarBatch> execute(
      Plan plan, Engine engine, ExecutorService ioExecutor) {
    return execute(plan, engine, ioExecutor, false);
  }

  /** Executes with a caller-owned executor and reports opt-in per-node measurements. */
  public static CloseableIterator<FilteredColumnarBatch> execute(
      Plan plan, Engine engine, ExecutorService ioExecutor, PlanExecutionObserver observer) {
    requireNonNull(observer, "observer is null");
    if (observer == PlanExecutionObserver.NOOP) {
      return execute(plan, engine, ioExecutor);
    }
    return execute(plan, engine, ioExecutor, false, observer);
  }

  private static CloseableIterator<FilteredColumnarBatch> execute(
      Plan plan, Engine engine, ExecutorService ioExecutor, boolean ownsExecutor) {
    return execute(plan, engine, ioExecutor, ownsExecutor, null);
  }

  private static CloseableIterator<FilteredColumnarBatch> execute(
      Plan plan,
      Engine engine,
      ExecutorService ioExecutor,
      boolean ownsExecutor,
      PlanExecutionObserver observer) {
    requireNonNull(plan, "plan is null");
    requireNonNull(engine, "engine is null");
    requireNonNull(ioExecutor, "ioExecutor is null");
    PlanExecutionInstrumentation instrumentation =
        observer == null
            ? null
            : new PlanExecutionInstrumentation(plan, observer, System::nanoTime);
    return new Execution(plan, engine, ioExecutor, ownsExecutor, instrumentation).execute();
  }

  private static int defaultIoParallelism() {
    int processors = Runtime.getRuntime().availableProcessors();
    return Math.max(4, Math.min(32, processors * 4));
  }

  private static final class Execution {
    private final Plan plan;
    private final Engine engine;
    private final ExecutorService ioExecutor;
    private final boolean ownsExecutor;
    private final PlanExecutionInstrumentation instrumentation;
    private final int[] fanout;
    private final List<ExecutionNode> compiled;
    private final List<SourceExecution> sources = new ArrayList<>();

    private Execution(
        Plan plan,
        Engine engine,
        ExecutorService ioExecutor,
        boolean ownsExecutor,
        PlanExecutionInstrumentation instrumentation) {
      this.plan = plan;
      this.engine = engine;
      this.ioExecutor = ioExecutor;
      this.ownsExecutor = ownsExecutor;
      this.instrumentation = instrumentation;
      int nodes = plan.getNodes().size();
      boolean[] reachable = reachableNodes(plan);
      this.fanout = countFanout(plan, reachable);
      this.compiled = new ArrayList<>(Collections.nCopies(nodes, null));
    }

    private CloseableIterator<FilteredColumnarBatch> execute() {
      try {
        ExecutionNode root = compile(plan.getNodes().size() - 1);
        root.prepareIo();
        CloseableIterator<FilteredColumnarBatch> terminal = root.execute();
        return new ResultIterator(
            terminal, sources, ownsExecutor ? ioExecutor : null, instrumentation);
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(failure, sources);
        if (ownsExecutor) {
          ioExecutor.shutdownNow();
        }
        finishAfterFailure(failure, instrumentation);
        throw failure;
      }
    }

    private ExecutionNode compile(int nodeIndex) {
      ExecutionNode existing = compiled.get(nodeIndex);
      if (existing != null) {
        return existing;
      }

      PlanNode node = plan.getNodes().get(nodeIndex);
      List<ExecutionNode> inputs = new ArrayList<>(node.getInputs().size());
      for (int inputIndex : node.getInputs()) {
        inputs.add(compile(inputIndex));
      }

      Operator operator = node.getOperator();
      ExecutionNode execution;
      if (operator instanceof ScanParquet) {
        ScanParquet scan = (ScanParquet) operator;
        SourceExecution source =
            new SourceExecution(
                nodeIndex,
                instrumentation,
                () -> FileScanExecutor.execute(scan, engine, ioExecutor));
        sources.add(source);
        execution = source;
      } else if (operator instanceof ScanJson) {
        ScanJson scan = (ScanJson) operator;
        SourceExecution source =
            new SourceExecution(
                nodeIndex,
                instrumentation,
                () -> FileScanExecutor.execute(scan, engine, ioExecutor));
        sources.add(source);
        execution = source;
      } else {
        execution =
            new OperatorExecution(
                nodeIndex, inputs, compileOperator(node, operator), instrumentation);
      }

      if (fanout[nodeIndex] > 1) {
        execution = new MaterializedExecution(execution);
      }
      compiled.set(nodeIndex, execution);
      return execution;
    }

    private BatchOperator compileOperator(PlanNode node, Operator operator) {
      if (operator instanceof Values) {
        Values values = (Values) operator;
        return ignored -> ValuesExecutor.execute(values);
      }
      if (operator instanceof Project) {
        Project project = (Project) operator;
        StructType inputSchema = inputSchema(node, 0);
        return inputs -> ProjectExecutor.execute(project, inputSchema, inputs.get(0));
      }
      if (operator instanceof Filter) {
        Filter filter = (Filter) operator;
        StructType inputSchema = inputSchema(node, 0);
        return inputs -> FilterExecutor.execute(filter, inputSchema, inputs.get(0));
      }
      if (operator instanceof Load) {
        Load load = (Load) operator;
        StructType inputSchema = inputSchema(node, 0);
        return inputs -> LoadExecutor.execute(load, inputSchema, inputs.get(0), engine, ioExecutor);
      }
      if (operator instanceof Aggregate) {
        Aggregate aggregate = (Aggregate) operator;
        StructType inputSchema = inputSchema(node, 0);
        return inputs -> AggregateExecutor.execute(aggregate, inputSchema, inputs.get(0));
      }
      if (operator instanceof SemiJoin) {
        SemiJoin join = (SemiJoin) operator;
        StructType probeSchema = inputSchema(node, 0);
        StructType buildSchema = inputSchema(node, 1);
        return inputs ->
            SemiJoinExecutor.execute(join, probeSchema, buildSchema, inputs.get(0), inputs.get(1));
      }
      if (operator instanceof UnionAll) {
        return UnionAllExecutor::execute;
      }
      throw new UnsupportedOperationException(
          "Unsupported plan operator: " + operator.getClass().getName());
    }

    private StructType inputSchema(PlanNode node, int inputPosition) {
      return plan.getOutputSchema(node.getInputs().get(inputPosition));
    }
  }

  /** Runtime graph node with an explicit prepare-I/O then execute lifecycle. */
  private abstract static class ExecutionNode {
    protected final List<ExecutionNode> inputs;
    protected final int nodeIndex;
    protected final PlanExecutionInstrumentation instrumentation;
    private boolean prepared;

    private ExecutionNode(
        List<ExecutionNode> inputs, int nodeIndex, PlanExecutionInstrumentation instrumentation) {
      this.inputs =
          Collections.unmodifiableList(new ArrayList<>(requireNonNull(inputs, "inputs is null")));
      this.nodeIndex = nodeIndex;
      this.instrumentation = instrumentation;
    }

    final void prepareIo() {
      if (prepared) {
        return;
      }
      boolean observed = instrumentation != null && nodeIndex >= 0;
      if (observed) {
        instrumentation.start(nodeIndex, PlanExecutionObserver.Phase.PREPARE);
      }
      try {
        for (ExecutionNode input : inputs) {
          input.prepareIo();
        }
        prepareOwnIo();
        prepared = true;
      } finally {
        if (observed) {
          instrumentation.stop();
        }
      }
    }

    protected void prepareOwnIo() {}

    abstract CloseableIterator<FilteredColumnarBatch> execute();
  }

  /** Static I/O source whose asynchronous iterator is created during the preparation pass. */
  private static final class SourceExecution extends ExecutionNode implements AutoCloseable {
    private final SourcePreparer preparer;
    private CloseableIterator<FilteredColumnarBatch> stream;
    private boolean executed;

    private SourceExecution(
        int nodeIndex, PlanExecutionInstrumentation instrumentation, SourcePreparer preparer) {
      super(Collections.emptyList(), nodeIndex, instrumentation);
      this.preparer = requireNonNull(preparer, "source preparer is null");
    }

    @Override
    protected void prepareOwnIo() {
      stream = requireNonNull(preparer.prepare(), "prepared source stream is null");
    }

    @Override
    CloseableIterator<FilteredColumnarBatch> execute() {
      if (instrumentation != null) {
        instrumentation.start(nodeIndex, PlanExecutionObserver.Phase.OPEN);
      }
      try {
        if (executed) {
          throw new IllegalStateException("Plan source was executed more than once");
        }
        executed = true;
        CloseableIterator<FilteredColumnarBatch> result =
            requireNonNull(stream, "Plan source I/O was not prepared");
        stream = null;
        return instrumentation == null ? result : instrumentation.observe(nodeIndex, result);
      } finally {
        if (instrumentation != null) {
          instrumentation.stop();
        }
      }
    }

    @Override
    public void close() throws IOException {
      if (stream != null) {
        CloseableIterator<FilteredColumnarBatch> toClose = stream;
        stream = null;
        toClose.close();
      }
    }
  }

  /** Ordinary operator with input traversal separated from its bound batch transformation. */
  private static final class OperatorExecution extends ExecutionNode {
    private final BatchOperator operator;
    private boolean executed;

    private OperatorExecution(
        int nodeIndex,
        List<ExecutionNode> inputs,
        BatchOperator operator,
        PlanExecutionInstrumentation instrumentation) {
      super(inputs, nodeIndex, instrumentation);
      this.operator = requireNonNull(operator, "batch operator is null");
    }

    @Override
    CloseableIterator<FilteredColumnarBatch> execute() {
      if (instrumentation != null) {
        instrumentation.start(nodeIndex, PlanExecutionObserver.Phase.OPEN);
      }
      try {
        if (executed) {
          throw new IllegalStateException(
              "Plan node " + nodeIndex + " was executed more than once");
        }
        executed = true;
        List<CloseableIterator<FilteredColumnarBatch>> streams = new ArrayList<>(inputs.size());
        try {
          for (ExecutionNode input : inputs) {
            streams.add(input.execute());
          }
          CloseableIterator<FilteredColumnarBatch> result = operator.execute(streams);
          return instrumentation == null ? result : instrumentation.observe(nodeIndex, result);
        } catch (RuntimeException | Error failure) {
          closeAfterFailure(failure, streams);
          throw failure;
        }
      } finally {
        if (instrumentation != null) {
          instrumentation.stop();
        }
      }
    }
  }

  /** Transparent replay boundary inserted only for a runtime node with multiple consumers. */
  private static final class MaterializedExecution extends ExecutionNode {
    private final ExecutionNode child;
    private List<FilteredColumnarBatch> batches;

    private MaterializedExecution(ExecutionNode child) {
      super(
          Collections.singletonList(requireNonNull(child, "materialized child is null")), -1, null);
      this.child = child;
    }

    @Override
    CloseableIterator<FilteredColumnarBatch> execute() {
      if (batches == null) {
        batches = child.execute().toInMemoryList();
      }
      return toCloseableIterator(batches.iterator());
    }
  }

  @FunctionalInterface
  private interface SourcePreparer {
    CloseableIterator<FilteredColumnarBatch> prepare();
  }

  @FunctionalInterface
  private interface BatchOperator {
    CloseableIterator<FilteredColumnarBatch> execute(
        List<CloseableIterator<FilteredColumnarBatch>> inputs);
  }

  private static boolean[] reachableNodes(Plan plan) {
    boolean[] reachable = new boolean[plan.getNodes().size()];
    reachable[reachable.length - 1] = true;
    for (int nodeIndex = reachable.length - 1; nodeIndex >= 0; nodeIndex--) {
      if (reachable[nodeIndex]) {
        for (int inputIndex : plan.getNodes().get(nodeIndex).getInputs()) {
          reachable[inputIndex] = true;
        }
      }
    }
    return reachable;
  }

  private static int[] countFanout(Plan plan, boolean[] reachable) {
    int[] fanout = new int[plan.getNodes().size()];
    for (int nodeIndex = 0; nodeIndex < plan.getNodes().size(); nodeIndex++) {
      if (reachable[nodeIndex]) {
        for (int inputIndex : plan.getNodes().get(nodeIndex).getInputs()) {
          fanout[inputIndex] = Math.addExact(fanout[inputIndex], 1);
        }
      }
    }
    return fanout;
  }

  private static void closeAfterFailure(
      Throwable failure, List<? extends AutoCloseable> closeables) {
    for (int index = closeables.size() - 1; index >= 0; index--) {
      AutoCloseable closeable = closeables.get(index);
      if (closeable == null) {
        continue;
      }
      try {
        closeable.close();
      } catch (Throwable closeFailure) {
        if (closeFailure != failure) {
          failure.addSuppressed(closeFailure);
        }
      }
    }
  }

  private static void finishAfterFailure(
      Throwable failure, PlanExecutionInstrumentation instrumentation) {
    if (instrumentation == null) {
      return;
    }
    try {
      instrumentation.finish();
    } catch (Throwable observerFailure) {
      if (observerFailure != failure) {
        failure.addSuppressed(observerFailure);
      }
    }
  }

  private static final class ResultIterator implements CloseableIterator<FilteredColumnarBatch> {
    private final CloseableIterator<FilteredColumnarBatch> delegate;
    private final List<SourceExecution> sources;
    private final ExecutorService ownedExecutor;
    private final PlanExecutionInstrumentation instrumentation;
    private boolean closed;

    private ResultIterator(
        CloseableIterator<FilteredColumnarBatch> delegate,
        List<SourceExecution> sources,
        ExecutorService ownedExecutor,
        PlanExecutionInstrumentation instrumentation) {
      this.delegate = requireNonNull(delegate, "terminal iterator is null");
      this.sources = sources;
      this.ownedExecutor = ownedExecutor;
      this.instrumentation = instrumentation;
    }

    @Override
    public boolean hasNext() {
      if (closed) {
        return false;
      }
      try {
        boolean hasNext = delegate.hasNext();
        if (!hasNext) {
          try {
            close();
          } catch (IOException failure) {
            throw new UncheckedIOException("Failed to close exhausted plan result", failure);
          }
        }
        return hasNext;
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(failure, Collections.singletonList(this));
        throw failure;
      }
    }

    @Override
    public FilteredColumnarBatch next() {
      if (closed) {
        throw new NoSuchElementException("Plan result is closed");
      }
      try {
        return delegate.next();
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(failure, Collections.singletonList(this));
        throw failure;
      }
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      List<AutoCloseable> closeables = new ArrayList<>();
      closeables.add(delegate);
      closeables.addAll(sources);
      try {
        if (instrumentation != null) {
          instrumentation.finish();
        }
      } finally {
        try {
          closeAll(closeables);
        } finally {
          if (ownedExecutor != null) {
            ownedExecutor.shutdownNow();
          }
        }
      }
    }

    private static void closeAll(List<? extends AutoCloseable> closeables) throws IOException {
      Throwable failure = null;
      for (AutoCloseable closeable : closeables) {
        try {
          closeable.close();
        } catch (Throwable closeFailure) {
          if (failure == null) {
            failure = closeFailure;
          } else if (closeFailure != failure) {
            failure.addSuppressed(closeFailure);
          }
        }
      }
      if (failure instanceof IOException) {
        throw (IOException) failure;
      }
      if (failure instanceof RuntimeException) {
        throw (RuntimeException) failure;
      }
      if (failure instanceof Error) {
        throw (Error) failure;
      }
      if (failure != null) {
        throw new IOException("Failed to close plan result", failure);
      }
    }
  }
}
