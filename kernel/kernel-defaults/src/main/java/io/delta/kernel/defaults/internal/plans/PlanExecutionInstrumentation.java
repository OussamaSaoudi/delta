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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.internal.plans.Operator;
import io.delta.kernel.internal.plans.Plan;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.LongSupplier;

/** Enabled-only plan instrumentation kept separate from the ordinary execution path. */
final class PlanExecutionInstrumentation {
  private static final PlanExecutionObserver.Phase[] PHASES = PlanExecutionObserver.Phase.values();

  private final PlanExecutionObserver observer;
  private final LongSupplier nanoTime;
  private final String[] operatorTypes;
  private final long[] invocations;
  private final long[] inclusiveDurationNs;
  private final long[] childDurationNs;
  private final long[] outputBatches;
  private final long[] outputPhysicalRows;
  private final long[] outputKnownSelectedRows;
  private final long[] outputUnknownSelectedBatches;
  private final ThreadLocal<CallStack> callStacks = ThreadLocal.withInitial(CallStack::new);
  private boolean finished;

  PlanExecutionInstrumentation(Plan plan, PlanExecutionObserver observer, LongSupplier nanoTime) {
    requireNonNull(plan, "plan is null");
    this.observer = requireNonNull(observer, "observer is null");
    this.nanoTime = requireNonNull(nanoTime, "nanoTime is null");
    this.operatorTypes = new String[plan.getNodes().size()];
    for (int index = 0; index < operatorTypes.length; index++) {
      Operator operator = plan.getNodes().get(index).getOperator();
      String simpleName = operator.getClass().getSimpleName();
      operatorTypes[index] = simpleName.isEmpty() ? operator.getClass().getName() : simpleName;
    }
    int slots = Math.multiplyExact(operatorTypes.length, PHASES.length);
    this.invocations = new long[slots];
    this.inclusiveDurationNs = new long[slots];
    this.childDurationNs = new long[slots];
    this.outputBatches = new long[slots];
    this.outputPhysicalRows = new long[slots];
    this.outputKnownSelectedRows = new long[slots];
    this.outputUnknownSelectedBatches = new long[slots];
  }

  void start(int nodeIndex, PlanExecutionObserver.Phase phase) {
    callStacks.get().push(slot(nodeIndex, phase), nanoTime.getAsLong());
  }

  void stop() {
    stop(null);
  }

  void stop(FilteredColumnarBatch output) {
    long endNs = nanoTime.getAsLong();
    CallStack stack = callStacks.get();
    int depth = stack.pop();
    int slot = stack.slots[depth];
    long inclusiveNs = endNs - stack.startNs[depth];
    long nestedNs = stack.childNs[depth];
    invocations[slot] = Math.addExact(invocations[slot], 1);
    inclusiveDurationNs[slot] = Math.addExact(inclusiveDurationNs[slot], inclusiveNs);
    childDurationNs[slot] = Math.addExact(childDurationNs[slot], nestedNs);
    if (depth > 0) {
      stack.childNs[depth - 1] = Math.addExact(stack.childNs[depth - 1], inclusiveNs);
    }
    if (output != null) {
      outputBatches[slot] = Math.addExact(outputBatches[slot], 1);
      outputPhysicalRows[slot] =
          Math.addExact(outputPhysicalRows[slot], output.getData().getSize());
      Optional<Integer> selectedRows = output.getPreComputedNumSelectedRows();
      if (selectedRows.isPresent()) {
        outputKnownSelectedRows[slot] =
            Math.addExact(outputKnownSelectedRows[slot], selectedRows.get());
      } else {
        outputUnknownSelectedBatches[slot] = Math.addExact(outputUnknownSelectedBatches[slot], 1);
      }
    }
  }

  CloseableIterator<FilteredColumnarBatch> observe(
      int nodeIndex, CloseableIterator<FilteredColumnarBatch> delegate) {
    return new ObservedIterator(nodeIndex, requireNonNull(delegate, "delegate is null"));
  }

  void finish() {
    if (finished) {
      return;
    }
    finished = true;
    callStacks.remove();
    for (int nodeIndex = 0; nodeIndex < operatorTypes.length; nodeIndex++) {
      for (PlanExecutionObserver.Phase phase : PHASES) {
        int slot = slot(nodeIndex, phase);
        if (invocations[slot] == 0) {
          continue;
        }
        observer.onMeasurement(
            new PlanExecutionObserver.Measurement(
                nodeIndex,
                operatorTypes[nodeIndex],
                phase,
                invocations[slot],
                inclusiveDurationNs[slot],
                childDurationNs[slot],
                outputBatches[slot],
                outputPhysicalRows[slot],
                outputKnownSelectedRows[slot],
                outputUnknownSelectedBatches[slot]));
      }
    }
  }

  private int slot(int nodeIndex, PlanExecutionObserver.Phase phase) {
    return Math.addExact(Math.multiplyExact(nodeIndex, PHASES.length), phase.ordinal());
  }

  private final class ObservedIterator implements CloseableIterator<FilteredColumnarBatch> {
    private final int nodeIndex;
    private final CloseableIterator<FilteredColumnarBatch> delegate;

    private ObservedIterator(int nodeIndex, CloseableIterator<FilteredColumnarBatch> delegate) {
      this.nodeIndex = nodeIndex;
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      start(nodeIndex, PlanExecutionObserver.Phase.HAS_NEXT);
      try {
        return delegate.hasNext();
      } finally {
        stop();
      }
    }

    @Override
    public FilteredColumnarBatch next() {
      FilteredColumnarBatch output = null;
      start(nodeIndex, PlanExecutionObserver.Phase.NEXT);
      try {
        output = delegate.next();
        return output;
      } finally {
        stop(output);
      }
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  private static final class CallStack {
    private int depth;
    private int[] slots = new int[16];
    private long[] startNs = new long[16];
    private long[] childNs = new long[16];

    private void push(int slot, long start) {
      if (depth == slots.length) {
        int newSize = Math.multiplyExact(depth, 2);
        slots = Arrays.copyOf(slots, newSize);
        startNs = Arrays.copyOf(startNs, newSize);
        childNs = Arrays.copyOf(childNs, newSize);
      }
      slots[depth] = slot;
      startNs[depth] = start;
      childNs[depth] = 0;
      depth++;
    }

    private int pop() {
      if (depth == 0) {
        throw new IllegalStateException("Plan measurement stack is empty");
      }
      return --depth;
    }
  }
}
