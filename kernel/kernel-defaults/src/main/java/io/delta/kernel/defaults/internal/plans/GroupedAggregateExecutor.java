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
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.plans.Agg;
import io.delta.kernel.internal.plans.Aggregate;
import io.delta.kernel.internal.util.ColumnBinding;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Hash aggregate for non-empty grouping keys. */
final class GroupedAggregateExecutor {
  private GroupedAggregateExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(
      Aggregate aggregate, StructType inputSchema, CloseableIterator<FilteredColumnarBatch> input) {
    requireNonNull(input, "input is null");
    requireNonNull(inputSchema, "inputSchema is null");
    StructType outputSchema =
        aggregate.getOutputSchema(Collections.singletonList(inputSchema));
    BoundAggregate bound = new BoundAggregate(aggregate, inputSchema, outputSchema);
    PlanValueUtils.KeyMap<GroupState> groups = new PlanValueUtils.KeyMap<>(bound.groupTypes);
    try {
      while (input.hasNext()) {
        FilteredColumnarBatch batch = requireNonNull(input.next(), "input batch is null");
        if (!inputSchema.equals(batch.getData().getSchema())) {
          throw new IllegalArgumentException("Aggregate input batch has an unexpected schema");
        }
        EvaluatedBatch evaluated = bound.evaluate(batch);
        for (int rowId = 0; rowId < batch.getData().getSize(); rowId++) {
          if (!batch.isSelected(rowId)) {
            continue;
          }
          GroupState state =
              groups.getOrInsert(
                  evaluated.groups,
                  rowId,
                  values -> new GroupState(values, bound.aggs.size()));
          state.update(bound.aggs, evaluated.aggs, rowId);
        }
      }

    } finally {
      Utils.closeCloseables(input);
      for (GroupState state : groups.values()) {
        state.close();
      }
    }

    List<Row> rows = new ArrayList<>(groups.size());
    for (GroupState state : groups.values()) {
      rows.add(GenericRow.fromValues(outputSchema, state.finish()));
    }
    FilteredColumnarBatch output =
        new FilteredColumnarBatch(
            new DefaultRowBasedColumnarBatch(outputSchema, rows), Optional.empty());
    return singletonCloseableIterator(output);
  }

  private static final class BoundAggregate {
    private final List<ColumnBinding> groups = new ArrayList<>();
    private final List<DataType> groupTypes = new ArrayList<>();
    private final List<BoundAgg> aggs = new ArrayList<>();

    private BoundAggregate(
        Aggregate aggregate, StructType inputSchema, StructType outputSchema) {
      for (int index = 0; index < aggregate.getGroupBy().size(); index++) {
        ColumnBinding group = ColumnBinding.resolve(inputSchema, aggregate.getGroupBy().get(index));
        PlanValueUtils.requireGroupType(group.getDataType());
        groups.add(group);
        groupTypes.add(group.getDataType());
      }
      for (int index = 0; index < aggregate.getAggs().size(); index++) {
        Agg agg = aggregate.getAggs().get(index);
        DataType valueType = outputSchema.at(groups.size() + index).getDataType();
        DataType keyType =
            agg.getKey()
                .map(column -> ColumnBinding.resolve(inputSchema, column).getDataType())
                .orElse(valueType);
        if (!supportsComparison(keyType)) {
          throw new UnsupportedOperationException(
              "Aggregate " + agg.getFunction() + " cannot order data type " + keyType);
        }
        aggs.add(new BoundAgg(agg, inputSchema, valueType, keyType));
      }
    }

    private EvaluatedBatch evaluate(FilteredColumnarBatch batch) {
      List<ColumnVector> groupValues = new ArrayList<>(groups.size());
      List<EvaluatedAgg> aggValues = new ArrayList<>(aggs.size());
      for (ColumnBinding group : groups) {
        groupValues.add(group.getVector(batch.getData()));
      }
      for (BoundAgg agg : aggs) {
        aggValues.add(agg.evaluate(batch));
      }
      return new EvaluatedBatch(groupValues, aggValues);
    }
  }

  private static final class BoundAgg {
    private final ColumnBinding value;
    private final Optional<ColumnBinding> key;
    private final DataType valueType;
    private final DataType keyType;
    private final boolean minimum;

    private BoundAgg(Agg agg, StructType schema, DataType valueType, DataType keyType) {
      this.value = ColumnBinding.resolve(schema, agg.getValue());
      this.key = agg.getKey().map(column -> ColumnBinding.resolve(schema, column));
      this.valueType = valueType;
      this.keyType = keyType;
      this.minimum =
          agg.getFunction() == Agg.Function.MIN
              || agg.getFunction() == Agg.Function.MIN_NON_NULL_BY;
    }

    private EvaluatedAgg evaluate(FilteredColumnarBatch batch) {
      ColumnVector values = value.getVector(batch.getData());
      ColumnVector keys = key.isPresent() ? key.get().getVector(batch.getData()) : values;
      return new EvaluatedAgg(values, keys);
    }
  }

  private static final class EvaluatedBatch {
    private final List<ColumnVector> groups;
    private final List<EvaluatedAgg> aggs;

    private EvaluatedBatch(List<ColumnVector> groups, List<EvaluatedAgg> aggs) {
      this.groups = groups;
      this.aggs = aggs;
    }
  }

  private static final class EvaluatedAgg {
    private final ColumnVector values;
    private final ColumnVector keys;

    private EvaluatedAgg(ColumnVector values, ColumnVector keys) {
      this.values = values;
      this.keys = keys;
    }
  }

  private static final class GroupState {
    private final Object[] values;
    private final ColumnVector[] ordering;
    private final int aggregateOffset;

    private GroupState(Object[] groups, int aggregateCount) {
      this.values = Arrays.copyOf(groups, groups.length + aggregateCount);
      this.ordering = new ColumnVector[aggregateCount];
      this.aggregateOffset = groups.length;
    }

    private void update(List<BoundAgg> bound, List<EvaluatedAgg> evaluated, int rowId) {
      for (int index = 0; index < bound.size(); index++) {
        BoundAgg agg = bound.get(index);
        EvaluatedAgg input = evaluated.get(index);
        if (input.values.isNullAt(rowId) || input.keys.isNullAt(rowId)) {
          continue;
        }
        int order =
            ordering[index] == null
                ? 1
                : compare(agg.keyType, input.keys, rowId, ordering[index], 0);
        if (ordering[index] == null || (agg.minimum ? order < 0 : order > 0)) {
          Object key = PlanValueUtils.materialize(input.keys, agg.keyType, rowId);
          Utils.closeCloseables(ordering[index]);
          ordering[index] =
              VectorUtils.buildColumnVector(Collections.singletonList(key), agg.keyType);
          values[aggregateOffset + index] =
              PlanValueUtils.materialize(input.values, agg.valueType, rowId);
        }
      }
    }

    private List<Object> finish() {
      return Arrays.asList(values);
    }

    private void close() {
      Utils.closeCloseables(ordering);
      Arrays.fill(ordering, null);
    }
  }

}
