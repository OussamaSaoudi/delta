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
import io.delta.kernel.internal.data.RowBackedColumnarBatch;
import io.delta.kernel.internal.util.RowKernels;
import io.delta.kernel.plans.Agg;
import io.delta.kernel.plans.PlanNode.Aggregate;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/** Hash aggregation over one fused operand projection. */
final class AggregateOperator {
  private static final int OUTPUT_BATCH_SIZE = 1024;

  private AggregateOperator() {}

  static CloseableIterator<ColumnarBatch> open(Aggregate node, OperatorDispatcher execution) {
    BoundAggregate bound = new BoundAggregate(node, execution.engine());
    CloseableIterator<ColumnarBatch> input = null;
    StateTable state = null;
    try {
      input = execution.open(node.input());
      state =
          bound.groupCount == 0
              ? StateTable.global(execution.engine(), bound.rowSlotCount)
              : StateTable.keyed(execution.engine(), bound.keySchema, bound.rowSlotCount);
      return new AggregateIterator(node, execution.engine(), bound, input, state);
    } catch (RuntimeException | Error failure) {
      ManagedIterator.closeAndSuppress(failure, bound, input);
      throw failure;
    }
  }

  private static final class AggregateIterator extends ManagedIterator<ColumnarBatch> {
    private final Aggregate node;
    private final PlanEngine engine;
    private final BoundAggregate bound;
    private final CloseableIterator<ColumnarBatch> input;
    private final StateTable state;
    private int outputOffset;
    private boolean drained;

    private AggregateIterator(
        Aggregate node,
        PlanEngine engine,
        BoundAggregate bound,
        CloseableIterator<ColumnarBatch> input,
        StateTable state) {
      super(bound, input);
      this.node = node;
      this.engine = engine;
      this.bound = bound;
      this.input = input;
      this.state = state;
    }

    @Override
    protected boolean hasNextOpen() {
      drain();
      return outputOffset < state.size();
    }

    @Override
    protected ColumnarBatch nextOpen() {
      int end = Math.min(outputOffset + OUTPUT_BATCH_SIZE, state.size());
      List<Row> keys = new ArrayList<>(end - outputOffset);
      List<List<Row>> values = new ArrayList<>(bound.aggregates.length);
      for (int aggregate = 0; aggregate < bound.aggregates.length; aggregate++) {
        values.add(new ArrayList<>(end - outputOffset));
      }
      for (int entryRef = outputOffset; entryRef < end; entryRef++) {
        keys.add(state.getKey(entryRef));
        for (int aggregate = 0; aggregate < bound.aggregates.length; aggregate++) {
          values.get(aggregate).add(bound.aggregates[aggregate].result(state, entryRef));
        }
      }

      ColumnarBatch output =
          new RowBackedColumnarBatch(bound.keySchema, keys, ColumnarBatch.Lifetime.OWNED);
      outputOffset = end;
      return engine.appendColumns(output, node.outputSchema(), values);
    }

    private void drain() {
      if (drained) {
        return;
      }
      while (input.hasNext()) {
        consume(input.next());
      }
      drained = true;
    }

    private void consume(ColumnarBatch batch) {
      ColumnarBatch operands = bound.evaluate(batch);
      for (int rowId = 0; rowId < batch.getSize(); rowId++) {
        Row row = operands == null ? null : RowKernels.rowAt(operands, rowId);
        int entryRef = 0;
        if (bound.groupCount != 0) {
          entryRef = state.probeOrInsert(row);
        }
        for (BoundAgg aggregate : bound.aggregates) {
          aggregate.update(state, entryRef, row);
        }
      }
    }
  }

  private static final class BoundAggregate implements AutoCloseable {
    private final int groupCount;
    private final int rowSlotCount;
    private final StructType keySchema;
    private final BoundAgg[] aggregates;
    private final BatchEvaluator evaluator;

    private BoundAggregate(Aggregate node, PlanEngine engine) {
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

      int nextRowSlot = 0;
      this.aggregates = new BoundAgg[node.aggregates().size()];
      for (int index = 0; index < aggregates.length; index++) {
        Agg aggregate = node.aggregates().get(index);
        int firstOperandOrdinal = operands.size();
        for (int operand = 0; operand < aggregate.operands().size(); operand++) {
          addOperand(
              operands,
              operandFields,
              "_agg_" + index + "_operand_" + operand,
              aggregate.operands().get(operand),
              aggregate.operandTypes().get(operand));
        }
        int outputRowSlot = -1;
        int orderingRowSlot = -1;
        if (!isLongAggregate(aggregate)) {
          outputRowSlot = nextRowSlot++;
          if (isNonNullBy(aggregate)) {
            orderingRowSlot = nextRowSlot++;
          }
        }
        aggregates[index] =
            new BoundAgg(engine, aggregate, outputRowSlot, orderingRowSlot, firstOperandOrdinal);
      }
      this.rowSlotCount = nextRowSlot;
      StructType operandSchema = new StructType(operandFields);
      this.evaluator =
          operands.isEmpty()
              ? null
              : engine.bind(
                  node.input().outputSchema(), new StructExpression(operands), operandSchema);
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
    private final PlanEngine engine;
    private final Agg.Function function;
    private final int outputRowSlot;
    private final int orderingRowSlot;
    private final LongState longState;
    private final int valueOrdinal;
    private final int sentinelOrdinal;
    private final int keyOrdinal;
    private final DataType valueType;
    private final DataType keyType;
    private final StructType valueSchema;
    private final StructType keySchema;

    private BoundAgg(
        PlanEngine engine,
        Agg aggregate,
        int outputRowSlot,
        int orderingRowSlot,
        int firstOperandOrdinal) {
      this.engine = engine;
      this.function = aggregate.function();
      this.outputRowSlot = outputRowSlot;
      this.orderingRowSlot = orderingRowSlot;
      this.longState = isLongAggregate(aggregate) ? new LongState() : null;
      this.valueOrdinal = aggregate.operands().isEmpty() ? -1 : firstOperandOrdinal;
      this.sentinelOrdinal = isNonNullBy(aggregate) ? firstOperandOrdinal + 1 : -1;
      this.keyOrdinal = isNonNullBy(aggregate) ? firstOperandOrdinal + 2 : -1;
      this.valueType = aggregate.operandTypes().isEmpty() ? null : aggregate.operandTypes().get(0);
      this.keyType = isNonNullBy(aggregate) ? aggregate.operandTypes().get(2) : valueType;
      this.valueSchema =
          new StructType()
              .add(new StructField("_value", aggregate.resultType(), aggregate.resultNullable()));
      this.keySchema =
          isNonNullBy(aggregate)
              ? new StructType().add(new StructField("_key", keyType, true))
              : null;
      if (function == Agg.Function.MIN || function == Agg.Function.MAX) {
        RowKernels.requireOrderable(valueType);
      }
      if (function == Agg.Function.MIN_NON_NULL_BY || function == Agg.Function.MAX_NON_NULL_BY) {
        RowKernels.requireOrderable(keyType);
      }
    }

    private void update(StateTable state, int entryRef, Row operands) {
      switch (function) {
        case MIN:
        case MAX:
          updateMinMax(state, entryRef, operands);
          return;
        case SUM:
          updateSum(state, entryRef, operands);
          return;
        case COUNT:
          if (!operands.isNullAt(valueOrdinal)) {
            longState.add(entryRef, 1);
          }
          return;
        case COUNT_STAR:
          longState.add(entryRef, 1);
          return;
        case MIN_NON_NULL_BY:
        case MAX_NON_NULL_BY:
          updateNonNullBy(state, entryRef, operands);
          return;
        default:
          throw new IllegalStateException("Unknown aggregate function " + function);
      }
    }

    private void updateMinMax(StateTable state, int entryRef, Row operands) {
      if (operands.isNullAt(valueOrdinal)) {
        return;
      }
      Row current = state.getValue(entryRef, outputRowSlot);
      if (current == null
          || improves(RowKernels.compare(valueType, operands, valueOrdinal, current, 0))) {
        state.setValue(
            entryRef, outputRowSlot, engine.retainValue(operands, valueOrdinal, valueSchema));
      }
    }

    private void updateSum(StateTable state, int entryRef, Row operands) {
      if (operands.isNullAt(valueOrdinal)) {
        return;
      }
      long value =
          valueType instanceof IntegerType
              ? operands.getInt(valueOrdinal)
              : operands.getLong(valueOrdinal);
      longState.add(entryRef, value);
    }

    private void updateNonNullBy(StateTable state, int entryRef, Row operands) {
      if (operands.isNullAt(sentinelOrdinal) || operands.isNullAt(keyOrdinal)) {
        return;
      }
      Row currentKey = state.getValue(entryRef, orderingRowSlot);
      if (currentKey == null
          || improves(RowKernels.compare(keyType, operands, keyOrdinal, currentKey, 0))) {
        state.setValue(
            entryRef, orderingRowSlot, engine.retainValue(operands, keyOrdinal, keySchema));
        state.setValue(
            entryRef, outputRowSlot, engine.retainValue(operands, valueOrdinal, valueSchema));
      }
    }

    private Row result(StateTable state, int entryRef) {
      if (function == Agg.Function.SUM
          || function == Agg.Function.COUNT
          || function == Agg.Function.COUNT_STAR) {
        if (function == Agg.Function.SUM && !longState.contains(entryRef)) {
          return null;
        }
        return engine.longValue(longState.get(entryRef), valueSchema);
      }
      return state.getValue(entryRef, outputRowSlot);
    }

    private boolean improves(int comparison) {
      return function == Agg.Function.MIN || function == Agg.Function.MIN_NON_NULL_BY
          ? comparison < 0
          : comparison > 0;
    }
  }

  private static boolean isNonNullBy(Agg aggregate) {
    return aggregate.function() == Agg.Function.MIN_NON_NULL_BY
        || aggregate.function() == Agg.Function.MAX_NON_NULL_BY;
  }

  private static boolean isLongAggregate(Agg aggregate) {
    return aggregate.function() == Agg.Function.SUM
        || aggregate.function() == Agg.Function.COUNT
        || aggregate.function() == Agg.Function.COUNT_STAR;
  }

  /** Primitive, columnar accumulator state indexed by hash-table entry. */
  private static final class LongState {
    private long[] values = new long[0];
    private final BitSet present = new BitSet();

    private void add(int entryRef, long value) {
      if (entryRef >= values.length) {
        values = Arrays.copyOf(values, Math.max(entryRef + 1, Math.max(8, values.length * 2)));
      }
      values[entryRef] = Math.addExact(values[entryRef], value);
      present.set(entryRef);
    }

    private long get(int entryRef) {
      return entryRef < values.length ? values[entryRef] : 0;
    }

    private boolean contains(int entryRef) {
      return present.get(entryRef);
    }
  }
}
