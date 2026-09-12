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
package io.delta.kernel.internal.execution;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.execution.PlanEngine;
import io.delta.kernel.execution.PlanEngine.BatchEvaluator;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.StructExpression;
import io.delta.kernel.internal.util.RowKernels;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.plans.PlanNode.SemiJoin;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.List;

/** Build-side hash set followed by streaming semi or anti filtering. */
final class SemiJoinOperator {
  private SemiJoinOperator() {}

  static CloseableIterator<ColumnarBatch> open(SemiJoin node, OperatorDispatcher execution) {
    PlanEngine engine = execution.engine();
    BoundKeys probeKeys =
        new BoundKeys(node.probe().outputSchema(), node.probeKeys(), node.keyTypes(), engine);
    BoundKeys buildKeys = null;
    CloseableIterator<ColumnarBatch> probe = null;
    CloseableIterator<ColumnarBatch> build = null;
    StateTable buildState = null;
    try {
      buildKeys =
          new BoundKeys(node.build().outputSchema(), node.buildKeys(), node.keyTypes(), engine);
      probe = execution.open(node.probe());
      build = execution.open(node.build());
      buildState = StateTable.keyed(engine, probeKeys.keySchema, 0);
      return new SemiJoinIterator(
          node.inverted(), engine, probeKeys, buildKeys, probe, build, buildState);
    } catch (RuntimeException | Error failure) {
      ManagedIterator.closeAndSuppress(failure, probeKeys, buildKeys, probe, build);
      throw failure;
    }
  }

  private static final class SemiJoinIterator extends ManagedIterator<ColumnarBatch> {
    private final boolean inverted;
    private final PlanEngine engine;
    private final BoundKeys probeKeys;
    private final BoundKeys buildKeys;
    private final CloseableIterator<ColumnarBatch> probe;
    private final CloseableIterator<ColumnarBatch> build;
    private final StateTable buildState;
    private boolean buildDrained;

    private SemiJoinIterator(
        boolean inverted,
        PlanEngine engine,
        BoundKeys probeKeys,
        BoundKeys buildKeys,
        CloseableIterator<ColumnarBatch> probe,
        CloseableIterator<ColumnarBatch> build,
        StateTable buildState) {
      super(probeKeys, buildKeys, probe, build);
      this.inverted = inverted;
      this.engine = engine;
      this.probeKeys = probeKeys;
      this.buildKeys = buildKeys;
      this.probe = probe;
      this.build = build;
      this.buildState = buildState;
    }

    @Override
    protected boolean hasNextOpen() {
      drainBuild();
      return probe.hasNext();
    }

    @Override
    protected ColumnarBatch nextOpen() {
      return filter(probe.next());
    }

    private void drainBuild() {
      if (buildDrained) {
        return;
      }
      while (build.hasNext()) {
        addBuildBatch(build.next());
      }
      Utils.closeCloseables(build);
      buildDrained = true;
    }

    private void addBuildBatch(ColumnarBatch batch) {
      ColumnarBatch keys = buildKeys.evaluate(batch);
      for (int rowId = 0; rowId < batch.getSize(); rowId++) {
        buildState.probeOrInsert(row(keys, rowId));
      }
    }

    private ColumnarBatch filter(ColumnarBatch batch) {
      ColumnarBatch keys = probeKeys.evaluate(batch);
      int size = batch.getSize();
      boolean[] selected = new boolean[size];
      for (int rowId = 0; rowId < size; rowId++) {
        boolean member = buildState.contains(row(keys, rowId));
        selected[rowId] = inverted != member;
      }
      return engine.filter(batch, selected);
    }

    private static Row row(ColumnarBatch batch, int rowId) {
      return batch == null ? null : RowKernels.rowAt(batch, rowId);
    }
  }

  private static final class BoundKeys implements AutoCloseable {
    private final StructType keySchema;
    private final BatchEvaluator evaluator;

    private BoundKeys(
        StructType inputSchema, List<Expression> keys, List<DataType> keyTypes, PlanEngine engine) {
      List<StructField> fields = new ArrayList<>(keyTypes.size());
      for (int index = 0; index < keyTypes.size(); index++) {
        fields.add(new StructField("_key_" + index, keyTypes.get(index), true));
      }
      this.keySchema = new StructType(fields);
      this.evaluator =
          keys.isEmpty() ? null : engine.bind(inputSchema, new StructExpression(keys), keySchema);
    }

    private ColumnarBatch evaluate(ColumnarBatch batch) {
      if (evaluator == null) {
        return null;
      }
      return evaluator.eval(batch);
    }

    @Override
    public void close() {
      if (evaluator != null) {
        evaluator.close();
      }
    }
  }
}
