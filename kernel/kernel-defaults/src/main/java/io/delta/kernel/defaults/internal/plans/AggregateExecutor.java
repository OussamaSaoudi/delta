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

import static io.delta.kernel.defaults.internal.expressions.DefaultExpressionUtils.compare;
import static io.delta.kernel.defaults.internal.expressions.DefaultExpressionUtils.supportsComparison;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.DefaultKernelUtils;
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.plans.Agg;
import io.delta.kernel.internal.plans.Aggregate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Materializes an ungrouped {@link Aggregate} using Kernel Java values. */
final class AggregateExecutor {
  private AggregateExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(
      Aggregate aggregate, StructType inputSchema, CloseableIterator<FilteredColumnarBatch> input) {
    StructType outputSchema =
        requireNonNull(aggregate, "aggregate is null")
            .getOutputSchema(
                Collections.singletonList(requireNonNull(inputSchema, "inputSchema is null")));
    requireNonNull(input, "input is null");
    List<BoundAgg> aggs = bind(aggregate, inputSchema, outputSchema);

    List<Object> values = new ArrayList<>(aggs.size());
    try {
      while (input.hasNext()) {
        FilteredColumnarBatch batch = requireNonNull(input.next(), "input batch is null");
        if (!inputSchema.equals(batch.getData().getSchema())) {
          throw new IllegalArgumentException(
              "Aggregate input batch schema "
                  + batch.getData().getSchema()
                  + " does not match expected schema "
                  + inputSchema);
        }
        for (BoundAgg agg : aggs) {
          agg.update(batch);
        }
      }
      for (BoundAgg agg : aggs) {
        values.add(agg.result());
      }
    } finally {
      List<AutoCloseable> closeables = new ArrayList<>(aggs);
      closeables.add(input);
      Utils.closeCloseables(closeables.toArray(new AutoCloseable[0]));
    }

    Row row = GenericRow.fromValues(outputSchema, values);
    FilteredColumnarBatch output =
        new FilteredColumnarBatch(
            new DefaultRowBasedColumnarBatch(outputSchema, Collections.singletonList(row)),
            Optional.empty());
    return singletonCloseableIterator(output);
  }

  private static List<BoundAgg> bind(
      Aggregate aggregate, StructType inputSchema, StructType outputSchema) {
    List<BoundAgg> result = new ArrayList<>(aggregate.getAggs().size());
    for (int index = 0; index < aggregate.getAggs().size(); index++) {
      Agg agg = aggregate.getAggs().get(index);
      DataType valueType = outputSchema.at(index).getDataType();
      DataType keyType =
          agg.getKey()
              .map(column -> DefaultKernelUtils.getDataType(inputSchema, column))
              .orElse(valueType);
      if (!supportsComparison(keyType)) {
        throw new UnsupportedOperationException(
            "Aggregate " + agg.getFunction() + " cannot order data type " + keyType);
      }
      result.add(new BoundAgg(agg, inputSchema, valueType, keyType));
    }
    return result;
  }

  private static final class BoundAgg implements AutoCloseable {
    private final ExpressionEvaluator value;
    private final Optional<ExpressionEvaluator> key;
    private final DataType valueType;
    private final DataType keyType;
    private final boolean minimum;
    private ColumnVector winningValues;
    private ColumnVector winningKeys;
    private int winningRow = -1;

    private BoundAgg(Agg agg, StructType inputSchema, DataType valueType, DataType keyType) {
      this.value = new DefaultExpressionEvaluator(inputSchema, agg.getValue(), valueType);
      this.key =
          agg.getKey().map(column -> new DefaultExpressionEvaluator(inputSchema, column, keyType));
      this.valueType = valueType;
      this.keyType = keyType;
      this.minimum =
          agg.getFunction() == Agg.Function.MIN
              || agg.getFunction() == Agg.Function.MIN_NON_NULL_BY;
    }

    private void update(FilteredColumnarBatch batch) {
      ColumnVector values = value.eval(batch.getData());
      ColumnVector keys = null;
      boolean retained = false;
      try {
        keys = key.isPresent() ? key.get().eval(batch.getData()) : values;
        int candidate = -1;
        for (int rowId = 0; rowId < batch.getData().getSize(); rowId++) {
          if (!batch.isSelected(rowId)
              || values.isNullAt(rowId)
              || keys.isNullAt(rowId)) {
            continue;
          }
          if (candidate < 0 || better(compare(keyType, keys, rowId, keys, candidate))) {
            candidate = rowId;
          }
        }
        if (candidate >= 0
            && (winningRow < 0
                || better(compare(keyType, keys, candidate, winningKeys, winningRow)))) {
          closeWinner();
          winningValues = values;
          winningKeys = keys;
          winningRow = candidate;
          retained = true;
        }
      } finally {
        if (!retained) {
          closeVectors(values, keys);
        }
      }
    }

    private boolean better(int comparison) {
      return minimum ? comparison < 0 : comparison > 0;
    }

    private Object result() {
      return winningRow < 0
          ? null
          : VectorUtils.getValueAsObject(winningValues, valueType, winningRow);
    }

    private void closeWinner() {
      closeVectors(winningValues, winningKeys);
      winningValues = null;
      winningKeys = null;
      winningRow = -1;
    }

    private static void closeVectors(ColumnVector values, ColumnVector keys) {
      if (values == null) {
        return;
      }
      if (keys != null && keys != values) {
        Utils.closeCloseables(values, keys);
      } else {
        values.close();
      }
    }

    @Override
    public void close() {
      closeWinner();
      if (key.isPresent()) {
        Utils.closeCloseables(value, key.get());
      } else {
        value.close();
      }
    }
  }
}
