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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.StructExpression;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.plans.SemiJoin;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/** Build-side hash set followed by streaming semi or anti filtering. */
final class SemiJoinOperator {
  private SemiJoinOperator() {}

  static CloseableIterator<FilteredColumnarBatch> open(SemiJoin node, ExecutionContext context) {
    ExpressionHandler expressions = context.expressions();
    BoundKeys probeKeys =
        new BoundKeys(node.probe().outputSchema(), node.probeKeys(), node.keyTypes(), expressions);
    BoundKeys buildKeys = null;
    CloseableIterator<FilteredColumnarBatch> probe = null;
    CloseableIterator<FilteredColumnarBatch> build = null;
    try {
      buildKeys =
          new BoundKeys(
              node.build().outputSchema(), node.buildKeys(), node.keyTypes(), expressions);
      probe = context.open(node.probe());
      build = context.open(node.build());
      return new SemiJoinIterator(node, expressions, probeKeys, buildKeys, probe, build);
    } catch (RuntimeException | Error failure) {
      Utils.closeCloseablesAndAddSuppressed(failure, probeKeys, buildKeys, probe, build);
      throw failure;
    }
  }

  private static final class SemiJoinIterator implements CloseableIterator<FilteredColumnarBatch> {
    private final SemiJoin node;
    private final ExpressionHandler expressions;
    private final BoundKeys probeKeys;
    private final BoundKeys buildKeys;
    private final CloseableIterator<FilteredColumnarBatch> probe;
    private final CloseableIterator<FilteredColumnarBatch> build;
    private final StateTable buildState;
    private boolean buildDrained;
    private boolean closed;

    private SemiJoinIterator(
        SemiJoin node,
        ExpressionHandler expressions,
        BoundKeys probeKeys,
        BoundKeys buildKeys,
        CloseableIterator<FilteredColumnarBatch> probe,
        CloseableIterator<FilteredColumnarBatch> build) {
      this.node = node;
      this.expressions = expressions;
      this.probeKeys = probeKeys;
      this.buildKeys = buildKeys;
      this.probe = probe;
      this.build = build;
      this.buildState = StateTable.keyed(probeKeys.keySchema, 0);
    }

    @Override
    public boolean hasNext() {
      requireOpen();
      drainBuild();
      try {
        if (probe.hasNext()) {
          return true;
        }
        close();
        return false;
      } catch (RuntimeException | Error failure) {
        fail(failure);
        throw failure;
      }
    }

    @Override
    public FilteredColumnarBatch next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        return filter(probe.next());
      } catch (RuntimeException | Error failure) {
        fail(failure);
        throw failure;
      }
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      Utils.closeCloseables(probe, build, probeKeys, buildKeys, buildState);
    }

    private void drainBuild() {
      if (buildDrained) {
        return;
      }
      try {
        while (build.hasNext()) {
          addBuildBatch(build.next());
        }
        Utils.closeCloseables(build, buildKeys);
        buildDrained = true;
      } catch (RuntimeException | Error failure) {
        fail(failure);
        throw failure;
      }
    }

    private void addBuildBatch(FilteredColumnarBatch batch) {
      validateBatch("build", node.build().outputSchema(), batch);
      ColumnarBatch keys = buildKeys.evaluate(batch, "build");
      for (int rowId = 0; rowId < batch.getData().getSize(); rowId++) {
        if (!batch.isSelected(rowId)) {
          continue;
        }
        buildState.probeOrInsert(buildKeys.keyAt(keys, rowId));
      }
    }

    private FilteredColumnarBatch filter(FilteredColumnarBatch batch) {
      validateBatch("probe", node.probe().outputSchema(), batch);
      ColumnarBatch keys = probeKeys.evaluate(batch, "probe");
      int size = batch.getData().getSize();
      boolean[] selected = new boolean[size];
      for (int rowId = 0; rowId < size; rowId++) {
        if (batch.isSelected(rowId)) {
          boolean member = buildState.contains(probeKeys.keyAt(keys, rowId));
          selected[rowId] = node.inverted() != member;
        }
      }
      ColumnVector selection = expressions.createSelectionVector(selected, 0, size);
      return batch.withSelectionVector(selection, batch.getLifetime());
    }

    private void fail(Throwable failure) {
      closed = true;
      Utils.closeCloseablesAndAddSuppressed(
          failure, probe, build, probeKeys, buildKeys, buildState);
    }

    private void requireOpen() {
      if (closed) {
        throw new IllegalStateException("SemiJoin iterator is closed");
      }
    }
  }

  private static final class BoundKeys implements AutoCloseable {
    private final StructType keySchema;
    private final MutableBatchRow probe;
    private final Row emptyKey;
    private final ExpressionEvaluator evaluator;

    private BoundKeys(
        StructType inputSchema,
        List<Expression> keys,
        List<DataType> keyTypes,
        ExpressionHandler expressions) {
      List<StructField> fields = new ArrayList<>(keyTypes.size());
      for (int index = 0; index < keyTypes.size(); index++) {
        fields.add(new StructField("_key_" + index, keyTypes.get(index), true));
      }
      this.keySchema = new StructType(fields);
      this.probe = new MutableBatchRow(keySchema);
      this.emptyKey = keys.isEmpty() ? GenericRow.fromOwnedValues(keySchema, new Object[0]) : null;
      this.evaluator =
          keys.isEmpty()
              ? null
              : expressions.getEvaluator(inputSchema, new StructExpression(keys), keySchema);
    }

    private ColumnarBatch evaluate(FilteredColumnarBatch batch, String side) {
      if (evaluator == null) {
        return null;
      }
      FilteredColumnarBatch result = evaluator.eval(batch);
      if (!keySchema.equals(result.getData().getSchema())
          || result.getData().getSize() != batch.getData().getSize()) {
        throw new IllegalStateException(
            "SemiJoin " + side + " evaluator returned an unaligned batch");
      }
      return result.getData();
    }

    private Row keyAt(ColumnarBatch keys, int rowId) {
      if (evaluator == null) {
        return emptyKey;
      }
      probe.pointTo(keys, rowId);
      return probe;
    }

    @Override
    public void close() {
      Utils.closeCloseables(evaluator);
    }
  }

  private static void validateBatch(
      String side, StructType expectedSchema, FilteredColumnarBatch batch) {
    if (!expectedSchema.equals(batch.getData().getSchema())) {
      throw new IllegalArgumentException("SemiJoin " + side + " batch has an unexpected schema");
    }
  }
}
