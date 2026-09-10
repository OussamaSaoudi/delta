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
import io.delta.kernel.internal.data.GenericColumnVector;
import io.delta.kernel.internal.data.RowBackedColumnarBatch;
import io.delta.kernel.internal.util.RowKernels;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.plans.Agg;
import io.delta.kernel.plans.Aggregate;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

/** Hash aggregation over one fused operand projection. */
final class AggregateOperator {
  private static final int OUTPUT_BATCH_SIZE = 1024;

  private AggregateOperator() {}

  static CloseableIterator<FilteredColumnarBatch> open(Aggregate node, ExecutionContext context) {
    BoundAggregate bound = new BoundAggregate(node, context.expressions());
    CloseableIterator<FilteredColumnarBatch> input = null;
    try {
      input = context.open(node.input());
      return new AggregateIterator(node, bound, input);
    } catch (RuntimeException | Error failure) {
      Utils.closeCloseablesAndAddSuppressed(failure, input, bound);
      throw failure;
    }
  }

  private static final class AggregateIterator implements CloseableIterator<FilteredColumnarBatch> {
    private final Aggregate node;
    private final BoundAggregate bound;
    private final CloseableIterator<FilteredColumnarBatch> input;
    private final StateTable state;
    private int outputOffset;
    private boolean drained;
    private boolean closed;

    private AggregateIterator(
        Aggregate node, BoundAggregate bound, CloseableIterator<FilteredColumnarBatch> input) {
      this.node = node;
      this.bound = bound;
      this.input = input;
      this.state =
          bound.groupCount == 0
              ? StateTable.global(bound.valueSlotCount)
              : StateTable.keyed(bound.keySchema, bound.valueSlotCount);
    }

    @Override
    public boolean hasNext() {
      requireOpen();
      drain();
      if (outputOffset < state.size()) {
        return true;
      }
      close();
      return false;
    }

    @Override
    public FilteredColumnarBatch next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      int end = Math.min(outputOffset + OUTPUT_BATCH_SIZE, state.size());
      List<Row> keys = new ArrayList<>(end - outputOffset);
      List<List<Object>> values = new ArrayList<>(bound.aggregates.length);
      for (int aggregate = 0; aggregate < bound.aggregates.length; aggregate++) {
        values.add(new ArrayList<>(end - outputOffset));
      }
      for (int entryRef = outputOffset; entryRef < end; entryRef++) {
        keys.add(state.getKey(entryRef));
        for (int aggregate = 0; aggregate < bound.aggregates.length; aggregate++) {
          values.get(aggregate).add(state.getValue(entryRef, aggregate));
        }
      }

      ColumnarBatch output = new RowBackedColumnarBatch(bound.keySchema, keys);
      for (int aggregate = 0; aggregate < bound.aggregates.length; aggregate++) {
        StructField field = node.outputSchema().at(bound.groupCount + aggregate);
        output =
            output.withNewColumn(
                output.getSchema().length(),
                field,
                new GenericColumnVector(values.get(aggregate), field.getDataType()));
      }
      outputOffset = end;
      return new FilteredColumnarBatch(
          output, Optional.empty(), FilteredColumnarBatch.Lifetime.OWNED);
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      Utils.closeCloseables(input, bound, state);
    }

    private void drain() {
      if (drained) {
        return;
      }
      try {
        while (input.hasNext()) {
          consume(input.next());
        }
        bound.finishCounts(state);
        Utils.closeCloseables(input, bound);
        drained = true;
      } catch (RuntimeException | Error failure) {
        closed = true;
        Utils.closeCloseablesAndAddSuppressed(failure, input, bound, state);
        throw failure;
      }
    }

    private void consume(FilteredColumnarBatch batch) {
      if (!node.input().outputSchema().equals(batch.getData().getSchema())) {
        throw new IllegalArgumentException("Aggregate input batch has an unexpected schema");
      }
      ColumnarBatch operands = bound.evaluate(batch);
      for (int rowId = 0; rowId < batch.getData().getSize(); rowId++) {
        if (!batch.isSelected(rowId)) {
          continue;
        }
        int entryRef = 0;
        if (bound.groupCount != 0) {
          bound.probe.pointTo(operands, rowId);
          entryRef = state.probeOrInsert(bound.probe);
        }
        for (BoundAgg aggregate : bound.aggregates) {
          aggregate.update(state, entryRef, operands, rowId);
        }
      }
    }

    private void requireOpen() {
      if (closed) {
        throw new IllegalStateException("Aggregate iterator is closed");
      }
    }
  }

  private static final class BoundAggregate implements AutoCloseable {
    private final int groupCount;
    private final int valueSlotCount;
    private final StructType keySchema;
    private final StructType operandSchema;
    private final BoundAgg[] aggregates;
    private final MutableBatchRow probe;
    private final ExpressionEvaluator evaluator;

    private BoundAggregate(Aggregate node, ExpressionHandler expressions) {
      this.groupCount = node.groupBy().size();
      List<Expression> operands = new ArrayList<>();
      List<StructField> operandFields = new ArrayList<>();
      List<StructField> keyFields = new ArrayList<>();
      for (int index = 0; index < groupCount; index++) {
        DataType type = node.outputSchema().at(index).getDataType();
        StructField field = field("_group_" + index, type);
        operands.add(node.groupBy().get(index));
        operandFields.add(field);
        keyFields.add(node.outputSchema().at(index));
      }
      this.keySchema = new StructType(keyFields);
      this.probe = new MutableBatchRow(keySchema);

      int nextKeySlot = node.aggregates().size();
      this.aggregates = new BoundAgg[node.aggregates().size()];
      for (int index = 0; index < aggregates.length; index++) {
        Agg aggregate = node.aggregates().get(index);
        int valueOrdinal = -1;
        int sentinelOrdinal = -1;
        int keyOrdinal = -1;
        if (aggregate.value().isPresent()) {
          valueOrdinal =
              addOperand(
                  operands,
                  operandFields,
                  "_agg_" + index + "_value",
                  aggregate.value().get(),
                  aggregate.valueType().get());
        }
        if (aggregate.nullSentinel().isPresent()) {
          sentinelOrdinal =
              addOperand(
                  operands,
                  operandFields,
                  "_agg_" + index + "_sentinel",
                  aggregate.nullSentinel().get(),
                  aggregate.nullSentinelType().get());
        }
        if (aggregate.key().isPresent()) {
          keyOrdinal =
              addOperand(
                  operands,
                  operandFields,
                  "_agg_" + index + "_key",
                  aggregate.key().get(),
                  aggregate.keyType().get());
        }
        int orderingSlot = isNonNullBy(aggregate) ? nextKeySlot++ : index;
        aggregates[index] =
            new BoundAgg(aggregate, index, orderingSlot, valueOrdinal, sentinelOrdinal, keyOrdinal);
      }
      this.valueSlotCount = nextKeySlot;
      this.operandSchema = new StructType(operandFields);
      this.evaluator =
          operands.isEmpty()
              ? null
              : expressions.getEvaluator(
                  node.input().outputSchema(), new StructExpression(operands), operandSchema);
    }

    private ColumnarBatch evaluate(FilteredColumnarBatch batch) {
      if (evaluator == null) {
        return null;
      }
      FilteredColumnarBatch result = evaluator.eval(batch);
      if (!operandSchema.equals(result.getData().getSchema())
          || result.getData().getSize() != batch.getData().getSize()) {
        throw new IllegalStateException("Aggregate evaluator returned an unaligned batch");
      }
      return result.getData();
    }

    private void finishCounts(StateTable state) {
      for (int entryRef = 0; entryRef < state.size(); entryRef++) {
        for (BoundAgg aggregate : aggregates) {
          if (aggregate.isCount() && state.getValue(entryRef, aggregate.outputSlot) == null) {
            state.setValue(entryRef, aggregate.outputSlot, 0L);
          }
        }
      }
    }

    @Override
    public void close() {
      Utils.closeCloseables(evaluator);
    }

    private static int addOperand(
        List<Expression> expressions,
        List<StructField> fields,
        String name,
        Expression expression,
        DataType type) {
      int ordinal = expressions.size();
      expressions.add(expression);
      fields.add(field(name, type));
      return ordinal;
    }

    private static StructField field(String name, DataType type) {
      return new StructField(name, type, true);
    }
  }

  private static final class BoundAgg {
    private final Agg.Function function;
    private final int outputSlot;
    private final int orderingSlot;
    private final int valueOrdinal;
    private final int sentinelOrdinal;
    private final int keyOrdinal;
    private final DataType valueType;
    private final DataType keyType;

    private BoundAgg(
        Agg aggregate,
        int outputSlot,
        int orderingSlot,
        int valueOrdinal,
        int sentinelOrdinal,
        int keyOrdinal) {
      this.function = aggregate.function();
      this.outputSlot = outputSlot;
      this.orderingSlot = orderingSlot;
      this.valueOrdinal = valueOrdinal;
      this.sentinelOrdinal = sentinelOrdinal;
      this.keyOrdinal = keyOrdinal;
      this.valueType = aggregate.valueType().orElse(null);
      this.keyType = aggregate.keyType().orElse(valueType);
      if (function == Agg.Function.SUM && !LongType.LONG.equals(valueType)) {
        throw new IllegalArgumentException("SUM requires LONG input");
      }
      if (function == Agg.Function.MIN || function == Agg.Function.MAX) {
        RowKernels.requireOrderable(valueType);
      }
      if (function == Agg.Function.MIN_NON_NULL_BY || function == Agg.Function.MAX_NON_NULL_BY) {
        RowKernels.requireOrderable(keyType);
      }
    }

    private void update(StateTable state, int entryRef, ColumnarBatch operands, int rowId) {
      switch (function) {
        case MIN:
        case MAX:
          updateMinMax(state, entryRef, operands.getColumnVector(valueOrdinal), rowId);
          return;
        case SUM:
          updateSum(state, entryRef, operands.getColumnVector(valueOrdinal), rowId);
          return;
        case COUNT:
          updateCount(state, entryRef, operands.getColumnVector(valueOrdinal), rowId);
          return;
        case COUNT_STAR:
          increment(state, entryRef);
          return;
        case MIN_NON_NULL_BY:
        case MAX_NON_NULL_BY:
          updateNonNullBy(state, entryRef, operands, rowId);
          return;
        default:
          throw new IllegalStateException("Unknown aggregate function " + function);
      }
    }

    private void updateMinMax(StateTable state, int entryRef, ColumnVector values, int rowId) {
      if (values.isNullAt(rowId)) {
        return;
      }
      Object current = state.getValue(entryRef, outputSlot);
      if (current == null || improves(RowKernels.compare(valueType, values, rowId, current))) {
        state.setValue(entryRef, outputSlot, RowKernels.materialize(values, valueType, rowId));
      }
    }

    private void updateSum(StateTable state, int entryRef, ColumnVector values, int rowId) {
      if (values.isNullAt(rowId)) {
        return;
      }
      Long current = (Long) state.getValue(entryRef, outputSlot);
      long sum = Math.addExact(current == null ? 0L : current, values.getLong(rowId));
      state.setValue(entryRef, outputSlot, sum);
    }

    private void updateCount(StateTable state, int entryRef, ColumnVector values, int rowId) {
      if (!values.isNullAt(rowId)) {
        increment(state, entryRef);
      }
    }

    private void increment(StateTable state, int entryRef) {
      Long current = (Long) state.getValue(entryRef, outputSlot);
      state.setValue(entryRef, outputSlot, Math.addExact(current == null ? 0L : current, 1L));
    }

    private void updateNonNullBy(
        StateTable state, int entryRef, ColumnarBatch operands, int rowId) {
      ColumnVector sentinel = operands.getColumnVector(sentinelOrdinal);
      ColumnVector keys = operands.getColumnVector(keyOrdinal);
      if (sentinel.isNullAt(rowId) || keys.isNullAt(rowId)) {
        return;
      }
      Object currentKey = state.getValue(entryRef, orderingSlot);
      if (currentKey == null || improves(RowKernels.compare(keyType, keys, rowId, currentKey))) {
        ColumnVector values = operands.getColumnVector(valueOrdinal);
        state.setValue(entryRef, orderingSlot, RowKernels.materialize(keys, keyType, rowId));
        state.setValue(entryRef, outputSlot, RowKernels.materialize(values, valueType, rowId));
      }
    }

    private boolean improves(int comparison) {
      return function == Agg.Function.MIN || function == Agg.Function.MIN_NON_NULL_BY
          ? comparison < 0
          : comparison > 0;
    }

    private boolean isCount() {
      return function == Agg.Function.COUNT || function == Agg.Function.COUNT_STAR;
    }
  }

  private static boolean isNonNullBy(Agg aggregate) {
    return aggregate.function() == Agg.Function.MIN_NON_NULL_BY
        || aggregate.function() == Agg.Function.MAX_NON_NULL_BY;
  }
}
