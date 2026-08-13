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

  /** Executes with a caller-owned executor. */
  static CloseableIterator<FilteredColumnarBatch> execute(
      Plan plan, Engine engine, ExecutorService ioExecutor) {
    return execute(plan, engine, ioExecutor, false);
  }

  private static CloseableIterator<FilteredColumnarBatch> execute(
      Plan plan, Engine engine, ExecutorService ioExecutor, boolean ownsExecutor) {
    requireNonNull(plan, "plan is null");
    requireNonNull(engine, "engine is null");
    requireNonNull(ioExecutor, "ioExecutor is null");
    return new Execution(plan, engine, ioExecutor, ownsExecutor).execute();
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
    private final boolean[] reachable;
    private final int[] fanout;
    private final boolean[] opened;
    private final List<CloseableIterator<FilteredColumnarBatch>> prefetched;
    private final List<List<FilteredColumnarBatch>> materialized;

    private Execution(Plan plan, Engine engine, ExecutorService ioExecutor, boolean ownsExecutor) {
      this.plan = plan;
      this.engine = engine;
      this.ioExecutor = ioExecutor;
      this.ownsExecutor = ownsExecutor;
      int nodes = plan.getNodes().size();
      this.reachable = reachableNodes(plan);
      this.fanout = countFanout(plan, reachable);
      this.opened = new boolean[nodes];
      this.prefetched = new ArrayList<>(Collections.nCopies(nodes, null));
      this.materialized = new ArrayList<>(Collections.nCopies(nodes, null));
    }

    private CloseableIterator<FilteredColumnarBatch> execute() {
      try {
        validateReachableOperators();
        prefetchLeafScans();
        CloseableIterator<FilteredColumnarBatch> terminal = open(plan.getNodes().size() - 1);
        return new ResultIterator(terminal, prefetched, ownsExecutor ? ioExecutor : null);
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(failure, prefetched);
        if (ownsExecutor) {
          ioExecutor.shutdownNow();
        }
        throw failure;
      }
    }

    private void validateReachableOperators() {
      for (int nodeIndex = 0; nodeIndex < plan.getNodes().size(); nodeIndex++) {
        if (reachable[nodeIndex]) {
          validateSupported(plan.getNodes().get(nodeIndex).getOperator());
        }
      }
    }

    private void prefetchLeafScans() {
      for (int nodeIndex = 0; nodeIndex < plan.getNodes().size(); nodeIndex++) {
        if (!reachable[nodeIndex]) {
          continue;
        }
        Operator operator = plan.getNodes().get(nodeIndex).getOperator();
        if (operator instanceof ScanParquet) {
          prefetched.set(
              nodeIndex, FileScanExecutor.execute((ScanParquet) operator, engine, ioExecutor));
        } else if (operator instanceof ScanJson) {
          prefetched.set(
              nodeIndex, FileScanExecutor.execute((ScanJson) operator, engine, ioExecutor));
        }
      }
    }

    private CloseableIterator<FilteredColumnarBatch> open(int nodeIndex) {
      if (fanout[nodeIndex] <= 1) {
        return openOnce(nodeIndex);
      }

      List<FilteredColumnarBatch> batches = materialized.get(nodeIndex);
      if (batches == null) {
        batches = openOnce(nodeIndex).toInMemoryList();
        materialized.set(nodeIndex, batches);
      }
      return toCloseableIterator(batches.iterator());
    }

    private CloseableIterator<FilteredColumnarBatch> openOnce(int nodeIndex) {
      if (opened[nodeIndex]) {
        throw new IllegalStateException("Plan node " + nodeIndex + " was opened more than once");
      }
      opened[nodeIndex] = true;

      PlanNode node = plan.getNodes().get(nodeIndex);
      Operator operator = node.getOperator();
      if (operator instanceof ScanParquet || operator instanceof ScanJson) {
        CloseableIterator<FilteredColumnarBatch> scan = prefetched.set(nodeIndex, null);
        return requireNonNull(scan, "scan node was not prefetched");
      }

      List<CloseableIterator<FilteredColumnarBatch>> inputs = new ArrayList<>();
      try {
        for (int inputIndex : node.getInputs()) {
          inputs.add(open(inputIndex));
        }
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(failure, inputs);
        throw failure;
      }

      try {
        return dispatch(node, operator, inputs);
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(failure, inputs);
        throw failure;
      }
    }

    private CloseableIterator<FilteredColumnarBatch> dispatch(
        PlanNode node, Operator operator, List<CloseableIterator<FilteredColumnarBatch>> inputs) {
      if (operator instanceof Values) {
        return ValuesExecutor.execute((Values) operator);
      }
      if (operator instanceof Project) {
        return ProjectExecutor.execute((Project) operator, inputSchema(node, 0), inputs.get(0));
      }
      if (operator instanceof Filter) {
        return FilterExecutor.execute((Filter) operator, inputSchema(node, 0), inputs.get(0));
      }
      if (operator instanceof Load) {
        return LoadExecutor.execute(
            (Load) operator, inputSchema(node, 0), inputs.get(0), engine, ioExecutor);
      }
      if (operator instanceof Aggregate) {
        return AggregateExecutor.execute((Aggregate) operator, inputSchema(node, 0), inputs.get(0));
      }
      if (operator instanceof SemiJoin) {
        return SemiJoinExecutor.execute(
            (SemiJoin) operator,
            inputSchema(node, 0),
            inputSchema(node, 1),
            inputs.get(0),
            inputs.get(1));
      }
      if (operator instanceof UnionAll) {
        return UnionAllExecutor.execute(inputs);
      }
      throw new UnsupportedOperationException(
          "Unsupported plan operator: " + operator.getClass().getName());
    }

    private StructType inputSchema(PlanNode node, int inputPosition) {
      return plan.getOutputSchema(node.getInputs().get(inputPosition));
    }
  }

  private static void validateSupported(Operator operator) {
    if (operator instanceof Values
        || operator instanceof Project
        || operator instanceof Filter
        || operator instanceof Load
        || operator instanceof Aggregate
        || operator instanceof SemiJoin
        || operator instanceof UnionAll
        || operator instanceof ScanParquet
        || operator instanceof ScanJson) {
      return;
    }
    throw new UnsupportedOperationException(
        "Unsupported plan operator: " + operator.getClass().getName());
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

  private static final class ResultIterator implements CloseableIterator<FilteredColumnarBatch> {
    private final CloseableIterator<FilteredColumnarBatch> delegate;
    private final List<CloseableIterator<FilteredColumnarBatch>> prefetched;
    private final ExecutorService ownedExecutor;
    private boolean closed;

    private ResultIterator(
        CloseableIterator<FilteredColumnarBatch> delegate,
        List<CloseableIterator<FilteredColumnarBatch>> prefetched,
        ExecutorService ownedExecutor) {
      this.delegate = requireNonNull(delegate, "terminal iterator is null");
      this.prefetched = prefetched;
      this.ownedExecutor = ownedExecutor;
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
      for (CloseableIterator<FilteredColumnarBatch> scan : prefetched) {
        if (scan != null) {
          closeables.add(scan);
        }
      }
      try {
        closeAll(closeables);
      } finally {
        if (ownedExecutor != null) {
          ownedExecutor.shutdownNow();
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
