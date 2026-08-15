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

import static io.delta.kernel.defaults.internal.expressions.DefaultValueComparator.compare;
import static io.delta.kernel.defaults.internal.expressions.DefaultValueComparator.supports;
import static io.delta.kernel.defaults.internal.plans.PlanValueUtils.canonicalize;
import static io.delta.kernel.defaults.internal.plans.PlanValueUtils.materialize;
import static io.delta.kernel.defaults.internal.plans.PlanValueUtils.read;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.defaults.internal.data.DefaultValueRetainer;
import io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.plans.Agg;
import io.delta.kernel.internal.plans.Aggregate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Materializes a grouped or global {@link Aggregate} using Kernel Java values. */
final class AggregateExecutor {
  private AggregateExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(
      Aggregate aggregate, StructType inputSchema, CloseableIterator<FilteredColumnarBatch> input) {
    requireNonNull(aggregate, "aggregate is null");
    requireNonNull(inputSchema, "inputSchema is null");
    requireNonNull(input, "input is null");

    StructType outputSchema = aggregate.getOutputSchema(Collections.singletonList(inputSchema));
    BoundAggregate bound = BoundAggregate.bind(aggregate, inputSchema, outputSchema);
    boolean global = bound.groups.isEmpty();
    boolean singleGroup = bound.groups.size() == 1;
    Map<Object, GroupState> groups = global ? Collections.emptyMap() : new LinkedHashMap<>();
    GroupState globalState = global ? bound.newState(new Object[bound.aggs.size()]) : null;
    GroupKey lookupKey = global || singleGroup ? null : new GroupKey(bound.groups.size());

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
        try (EvaluatedBatch values = bound.evaluate(batch)) {
          for (int rowId = 0; rowId < batch.getData().getSize(); rowId++) {
            if (!batch.isSelected(rowId)) {
              continue;
            }
            GroupState state = globalState;
            if (!global) {
              Object key;
              if (singleGroup) {
                key = values.singleGroupKey(rowId);
              } else {
                values.setGroupKey(lookupKey, rowId);
                key = lookupKey;
              }
              state = groups.get(key);
              if (state == null) {
                state = bound.newState(values.readGroups(rowId, bound.aggs.size()));
                groups.put(singleGroup ? key : lookupKey.copy(), state);
              }
            }
            state.update(bound.aggs, values, rowId);
          }
        }
      }
    } finally {
      Utils.closeCloseables(bound, input);
    }

    Iterable<GroupState> states = global ? Collections.singletonList(globalState) : groups.values();
    int outputSize = global ? 1 : groups.size();
    List<Row> rows = new ArrayList<>(outputSize);
    for (GroupState state : states) {
      rows.add(GenericRow.fromValues(outputSchema, state.finish()));
    }
    FilteredColumnarBatch output =
        new FilteredColumnarBatch(
            new DefaultRowBasedColumnarBatch(outputSchema, rows), Optional.empty());
    return singletonCloseableIterator(output);
  }

  private static final class BoundAggregate implements AutoCloseable {
    private final List<BoundColumn> groups;
    private final List<DataType> groupTypes;
    private final List<BoundAgg> aggs;

    private BoundAggregate(
        List<BoundColumn> groups, List<DataType> groupTypes, List<BoundAgg> aggs) {
      this.groups = groups;
      this.groupTypes = groupTypes;
      this.aggs = aggs;
    }

    static BoundAggregate bind(
        Aggregate aggregate, StructType inputSchema, StructType outputSchema) {
      List<BoundColumn> groups = new ArrayList<>(aggregate.getGroupBy().size());
      List<DataType> groupTypes = new ArrayList<>(groups.size());
      for (int index = 0; index < aggregate.getGroupBy().size(); index++) {
        DataType type = outputSchema.at(index).getDataType();
        PlanValueUtils.validateCanonicalizable(type, "Aggregate group key", false);
        groups.add(BoundColumn.bind(inputSchema, aggregate.getGroupBy().get(index), type));
        groupTypes.add(type);
      }
      List<BoundAgg> aggs = new ArrayList<>(aggregate.getAggs().size());
      for (int index = 0; index < aggregate.getAggs().size(); index++) {
        DataType outputType = outputSchema.at(groups.size() + index).getDataType();
        aggs.add(BoundAgg.bind(inputSchema, aggregate.getAggs().get(index), outputType));
      }
      return new BoundAggregate(groups, groupTypes, aggs);
    }

    EvaluatedBatch evaluate(FilteredColumnarBatch batch) {
      List<ColumnVector> groupValues = new ArrayList<>(groups.size());
      List<EvaluatedAgg> aggValues = new ArrayList<>(aggs.size());
      try {
        for (BoundColumn group : groups) {
          groupValues.add(group.evaluator.eval(batch.getData()));
        }
        for (BoundAgg agg : aggs) {
          aggValues.add(agg.evaluate(batch));
        }
        return new EvaluatedBatch(groupValues, aggValues, groupTypes);
      } catch (RuntimeException failure) {
        List<AutoCloseable> closeables = new ArrayList<>(groupValues);
        closeables.addAll(aggValues);
        Utils.closeCloseablesAndAddSuppressed(failure, closeables.toArray(new AutoCloseable[0]));
        throw failure;
      }
    }

    GroupState newState(Object[] values) {
      return new GroupState(values, groups.size(), aggs.size());
    }

    @Override
    public void close() {
      List<AutoCloseable> evaluators = new ArrayList<>();
      for (BoundColumn group : groups) {
        evaluators.add(group.evaluator);
      }
      for (BoundAgg agg : aggs) {
        evaluators.add(agg.value.evaluator);
        agg.key.ifPresent(column -> evaluators.add(column.evaluator));
      }
      Utils.closeCloseables(evaluators.toArray(new AutoCloseable[0]));
    }
  }

  private static final class BoundColumn {
    private final ExpressionEvaluator evaluator;
    private final DataType type;

    private BoundColumn(ExpressionEvaluator evaluator, DataType type) {
      this.evaluator = evaluator;
      this.type = type;
    }

    static BoundColumn bind(StructType inputSchema, Column column, DataType type) {
      PlanValueUtils.validateMaterializable(type, "Aggregate column `" + column + "`");
      return new BoundColumn(new DefaultExpressionEvaluator(inputSchema, column, type), type);
    }
  }

  private static final class BoundAgg {
    private final boolean minimum;
    private final BoundColumn value;
    private final Optional<BoundColumn> key;

    private BoundAgg(Agg.Function function, BoundColumn value, Optional<BoundColumn> key) {
      this.minimum = function == Agg.Function.MIN || function == Agg.Function.MIN_NON_NULL_BY;
      this.value = value;
      this.key = key;
    }

    static BoundAgg bind(StructType inputSchema, Agg agg, DataType outputType) {
      BoundColumn value = BoundColumn.bind(inputSchema, agg.getValue(), outputType);
      Optional<BoundColumn> key =
          agg.getKey()
              .map(
                  column ->
                      BoundColumn.bind(inputSchema, column, resolveType(inputSchema, column)));
      DataType orderingType = key.map(column -> column.type).orElse(outputType);
      if (!supports(orderingType)) {
        throw new UnsupportedOperationException(
            "Aggregate " + agg.getFunction() + " cannot order data type " + orderingType);
      }
      return new BoundAgg(agg.getFunction(), value, key);
    }

    EvaluatedAgg evaluate(FilteredColumnarBatch batch) {
      ColumnVector values = value.evaluator.eval(batch.getData());
      try {
        ColumnVector keys = key.isPresent() ? key.get().evaluator.eval(batch.getData()) : values;
        return new EvaluatedAgg(
            values,
            keys,
            key.isPresent(),
            value.type,
            key.map(column -> column.type).orElse(value.type));
      } catch (RuntimeException failure) {
        values.close();
        throw failure;
      }
    }

    private static DataType resolveType(StructType schema, Column column) {
      DataType current = schema;
      for (String name : column.getNames()) {
        if (!(current instanceof StructType)) {
          throw new IllegalArgumentException("Cannot resolve aggregate column `" + column + "`");
        }
        StructType struct = (StructType) current;
        int ordinal = struct.indexOf(name);
        if (ordinal < 0) {
          throw new IllegalArgumentException("Cannot resolve aggregate column `" + column + "`");
        }
        current = struct.at(ordinal).getDataType();
      }
      return current;
    }
  }

  private static final class EvaluatedBatch implements AutoCloseable {
    private final List<ColumnVector> groups;
    private final List<EvaluatedAgg> aggs;
    private final List<DataType> groupTypes;

    private EvaluatedBatch(
        List<ColumnVector> groups, List<EvaluatedAgg> aggs, List<DataType> groupTypes) {
      this.groups = groups;
      this.aggs = aggs;
      this.groupTypes = groupTypes;
    }

    Object[] readGroups(int rowId, int aggregateCount) {
      Object[] values = new Object[groups.size() + aggregateCount];
      for (int index = 0; index < groups.size(); index++) {
        values[index] = materialize(groups.get(index), groupTypes.get(index), rowId);
      }
      return values;
    }

    void setGroupKey(GroupKey key, int rowId) {
      for (int index = 0; index < groups.size(); index++) {
        key.set(index, canonicalize(groups.get(index), groupTypes.get(index), rowId));
      }
      key.finish();
    }

    Object singleGroupKey(int rowId) {
      return canonicalize(groups.get(0), groupTypes.get(0), rowId);
    }

    @Override
    public void close() {
      List<AutoCloseable> closeables = new ArrayList<>(groups);
      closeables.addAll(aggs);
      Utils.closeCloseables(closeables.toArray(new AutoCloseable[0]));
    }
  }

  private static final class EvaluatedAgg implements AutoCloseable {
    private final ColumnVector values;
    private final ColumnVector keys;
    private final boolean separateKey;
    private final DataType valueType;
    private final DataType keyType;

    private EvaluatedAgg(
        ColumnVector values,
        ColumnVector keys,
        boolean separateKey,
        DataType valueType,
        DataType keyType) {
      this.values = values;
      this.keys = keys;
      this.separateKey = separateKey;
      this.valueType = valueType;
      this.keyType = keyType;
    }

    @Override
    public void close() {
      if (separateKey) {
        Utils.closeCloseables(values, keys);
      } else {
        values.close();
      }
    }
  }

  private static final class GroupState {
    private final Object[] values;
    private final Object[] orderingValues;
    private final int aggregateOffset;

    private GroupState(Object[] values, int groupCount, int aggregateCount) {
      this.values = values;
      this.orderingValues = new Object[aggregateCount];
      this.aggregateOffset = groupCount;
    }

    void update(List<BoundAgg> boundAggs, EvaluatedBatch batch, int rowId) {
      for (int index = 0; index < orderingValues.length; index++) {
        update(index, boundAggs.get(index), batch.aggs.get(index), rowId);
      }
    }

    List<Object> finish() {
      return Arrays.asList(values);
    }

    private void update(int index, BoundAgg bound, EvaluatedAgg agg, int rowId) {
      if (agg.values.isNullAt(rowId) || agg.keys.isNullAt(rowId)) {
        return;
      }
      Object ordering = read(agg.keys, agg.keyType, rowId);
      Object current = orderingValues[index];
      if (current == null
          || (bound.minimum && compare(agg.keyType, ordering, current) < 0)
          || (!bound.minimum && compare(agg.keyType, ordering, current) > 0)) {
        if (agg.separateKey) {
          values[aggregateOffset + index] =
              DefaultValueRetainer.retain(agg.values, agg.valueType, rowId);
          orderingValues[index] = retainScalar(ordering, agg.keyType);
        } else {
          Object retained = retainScalar(ordering, agg.valueType);
          values[aggregateOffset + index] = retained;
          orderingValues[index] = retained;
        }
      }
    }

    private static Object retainScalar(Object value, DataType type) {
      return type instanceof io.delta.kernel.types.BinaryType ? ((byte[]) value).clone() : value;
    }
  }

  private static final class GroupKey {
    private final Object[] values;
    private int hashCode;

    private GroupKey(int size) {
      this.values = new Object[size];
    }

    private GroupKey(Object[] values, int hashCode) {
      this.values = values;
      this.hashCode = hashCode;
    }

    void set(int index, Object value) {
      values[index] = value;
    }

    void finish() {
      hashCode = Arrays.hashCode(values);
    }

    GroupKey copy() {
      return new GroupKey(values.clone(), hashCode);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof GroupKey && Arrays.equals(values, ((GroupKey) other).values);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
