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

/** Receives immutable measurements from an instrumented plan execution. */
@FunctionalInterface
public interface PlanExecutionObserver {
  /** Disables instrumentation and preserves the ordinary execution path. */
  PlanExecutionObserver NOOP = ignored -> {};

  /** Called once for each node phase that ran, when the plan result is closed or exhausted. */
  void onMeasurement(Measurement measurement);

  /** A separately measured part of a plan node's lifecycle. */
  enum Phase {
    PREPARE,
    OPEN,
    HAS_NEXT,
    NEXT
  }

  /** Aggregated measurements for one plan node and lifecycle phase. */
  final class Measurement {
    private final int nodeIndex;
    private final String operatorType;
    private final Phase phase;
    private final long invocationCount;
    private final long inclusiveDurationNs;
    private final long childDurationNs;
    private final long outputBatchCount;
    private final long outputPhysicalRowCount;
    private final long outputKnownSelectedRowCount;
    private final long outputUnknownSelectedBatchCount;

    Measurement(
        int nodeIndex,
        String operatorType,
        Phase phase,
        long invocationCount,
        long inclusiveDurationNs,
        long childDurationNs,
        long outputBatchCount,
        long outputPhysicalRowCount,
        long outputKnownSelectedRowCount,
        long outputUnknownSelectedBatchCount) {
      this.nodeIndex = nodeIndex;
      this.operatorType = requireNonNull(operatorType, "operatorType is null");
      this.phase = requireNonNull(phase, "phase is null");
      this.invocationCount = invocationCount;
      this.inclusiveDurationNs = inclusiveDurationNs;
      this.childDurationNs = childDurationNs;
      this.outputBatchCount = outputBatchCount;
      this.outputPhysicalRowCount = outputPhysicalRowCount;
      this.outputKnownSelectedRowCount = outputKnownSelectedRowCount;
      this.outputUnknownSelectedBatchCount = outputUnknownSelectedBatchCount;
    }

    public int getNodeIndex() {
      return nodeIndex;
    }

    public String getOperatorType() {
      return operatorType;
    }

    public Phase getPhase() {
      return phase;
    }

    public long getInvocationCount() {
      return invocationCount;
    }

    /** Wall-clock time including nested plan-node calls. */
    public long getInclusiveDurationNs() {
      return inclusiveDurationNs;
    }

    /** Wall-clock time spent in nested plan-node calls. */
    public long getChildDurationNs() {
      return childDurationNs;
    }

    /** Wall-clock time excluding nested plan-node calls. */
    public long getSelfDurationNs() {
      return inclusiveDurationNs - childDurationNs;
    }

    public long getOutputBatchCount() {
      return outputBatchCount;
    }

    public long getOutputPhysicalRowCount() {
      return outputPhysicalRowCount;
    }

    /** Selected rows known without evaluating a selection vector. */
    public long getOutputKnownSelectedRowCount() {
      return outputKnownSelectedRowCount;
    }

    /** Output batches whose selected-row count was not already available. */
    public long getOutputUnknownSelectedBatchCount() {
      return outputUnknownSelectedBatchCount;
    }
  }
}
