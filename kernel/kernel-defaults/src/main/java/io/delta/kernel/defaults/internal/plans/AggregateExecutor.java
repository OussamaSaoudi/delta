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
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.plans.Agg;
import io.delta.kernel.internal.plans.Aggregate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.GeographyType;
import io.delta.kernel.types.GeometryType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
    Map<GroupKey, GroupState> groups = new LinkedHashMap<>();
    if (bound.groups.isEmpty()) {
      groups.put(new GroupKey(Collections.emptyList()), bound.newState(Collections.emptyList()));
    }

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
            if (!isSelected(batch, rowId)) {
              continue;
            }
            List<Object> groupValues = values.readGroups(rowId);
            GroupKey key = GroupKey.from(groupValues, bound.groupTypes);
            groups
                .computeIfAbsent(key, ignored -> bound.newState(groupValues))
                .update(values, rowId);
          }
        }
      }
    } finally {
      Utils.closeCloseables(bound, input);
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

  private static boolean isSelected(FilteredColumnarBatch batch, int rowId) {
    Optional<ColumnVector> selection = batch.getSelectionVector();
    return !selection.isPresent()
        || (!selection.get().isNullAt(rowId) && selection.get().getBoolean(rowId));
  }

  private static Object read(ColumnVector vector, DataType type, int rowId) {
    if (vector.isNullAt(rowId)) {
      return null;
    } else if (type instanceof BooleanType) {
      return vector.getBoolean(rowId);
    } else if (type instanceof ByteType) {
      return vector.getByte(rowId);
    } else if (type instanceof ShortType) {
      return vector.getShort(rowId);
    } else if (type instanceof IntegerType || type instanceof DateType) {
      return vector.getInt(rowId);
    } else if (type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType) {
      return vector.getLong(rowId);
    } else if (type instanceof FloatType) {
      return vector.getFloat(rowId);
    } else if (type instanceof DoubleType) {
      return vector.getDouble(rowId);
    } else if (type instanceof DecimalType) {
      return vector.getDecimal(rowId);
    } else if (type instanceof StringType
        || type instanceof GeometryType
        || type instanceof GeographyType) {
      return vector.getString(rowId);
    } else if (type instanceof BinaryType) {
      return vector.getBinary(rowId);
    } else if (type instanceof StructType) {
      return io.delta.kernel.internal.data.StructRow.fromStructVector(vector, rowId);
    } else if (type instanceof ArrayType) {
      return vector.getArray(rowId);
    } else if (type instanceof MapType) {
      return vector.getMap(rowId);
    }
    throw new UnsupportedOperationException("Aggregate cannot materialize data type " + type);
  }

  private static void validateMaterializable(DataType type, String context) {
    if (supports(type)) {
      return;
    }
    if (type instanceof StructType) {
      for (StructField field : ((StructType) type).fields()) {
        validateMaterializable(field.getDataType(), context);
      }
      return;
    }
    if (type instanceof ArrayType) {
      validateMaterializable(((ArrayType) type).getElementType(), context);
      return;
    }
    if (type instanceof MapType) {
      MapType map = (MapType) type;
      validateMaterializable(map.getKeyType(), context);
      validateMaterializable(map.getValueType(), context);
      return;
    }
    throw new UnsupportedOperationException(context + " has unsupported data type " + type);
  }

  private static void validateGroupType(DataType type) {
    if (type instanceof MapType) {
      throw new UnsupportedOperationException("Aggregate group keys cannot contain maps");
    }
    if (type instanceof StructType) {
      for (StructField field : ((StructType) type).fields()) {
        validateGroupType(field.getDataType());
      }
    } else if (type instanceof ArrayType) {
      validateGroupType(((ArrayType) type).getElementType());
    } else {
      validateMaterializable(type, "Aggregate group key");
    }
  }

  private static Object canonicalize(Object value, DataType type) {
    if (value == null) {
      return null;
    }
    if (type instanceof BinaryType) {
      return ByteBuffer.wrap(((byte[]) value).clone()).asReadOnlyBuffer();
    }
    if (type instanceof FloatType) {
      float number = (Float) value;
      return number == 0.0f ? 0.0f : (Float.isNaN(number) ? Float.NaN : number);
    }
    if (type instanceof DoubleType) {
      double number = (Double) value;
      return number == 0.0d ? 0.0d : (Double.isNaN(number) ? Double.NaN : number);
    }
    if (type instanceof StructType) {
      Row row = (Row) value;
      StructType struct = (StructType) type;
      List<Object> fields = new ArrayList<>(struct.length());
      for (int index = 0; index < struct.length(); index++) {
        DataType childType = struct.at(index).getDataType();
        fields.add(canonicalize(readRow(row, childType, index), childType));
      }
      return fields;
    }
    if (type instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) type;
      ArrayValue array = (ArrayValue) value;
      ColumnVector elements = array.getElements();
      List<Object> values = new ArrayList<>(array.getSize());
      for (int index = 0; index < array.getSize(); index++) {
        Object element = read(elements, arrayType.getElementType(), index);
        values.add(canonicalize(element, arrayType.getElementType()));
      }
      return values;
    }
    return value;
  }

  private static Object readRow(Row row, DataType type, int ordinal) {
    if (row.isNullAt(ordinal)) {
      return null;
    } else if (type instanceof BooleanType) {
      return row.getBoolean(ordinal);
    } else if (type instanceof ByteType) {
      return row.getByte(ordinal);
    } else if (type instanceof ShortType) {
      return row.getShort(ordinal);
    } else if (type instanceof IntegerType || type instanceof DateType) {
      return row.getInt(ordinal);
    } else if (type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType) {
      return row.getLong(ordinal);
    } else if (type instanceof FloatType) {
      return row.getFloat(ordinal);
    } else if (type instanceof DoubleType) {
      return row.getDouble(ordinal);
    } else if (type instanceof DecimalType) {
      return row.getDecimal(ordinal);
    } else if (type instanceof StringType
        || type instanceof GeometryType
        || type instanceof GeographyType) {
      return row.getString(ordinal);
    } else if (type instanceof BinaryType) {
      return row.getBinary(ordinal);
    } else if (type instanceof StructType) {
      return row.getStruct(ordinal);
    } else if (type instanceof ArrayType) {
      return row.getArray(ordinal);
    } else if (type instanceof MapType) {
      return row.getMap(ordinal);
    }
    throw new UnsupportedOperationException("Aggregate cannot materialize data type " + type);
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
        validateGroupType(type);
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
        try {
          Utils.closeCloseables(closeables.toArray(new AutoCloseable[0]));
        } catch (RuntimeException closeFailure) {
          failure.addSuppressed(closeFailure);
        }
        throw failure;
      }
    }

    GroupState newState(List<Object> groupValues) {
      return new GroupState(groupValues, aggs);
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
      validateMaterializable(type, "Aggregate column `" + column + "`");
      return new BoundColumn(new DefaultExpressionEvaluator(inputSchema, column, type), type);
    }
  }

  private static final class BoundAgg {
    private final Agg.Function function;
    private final BoundColumn value;
    private final Optional<BoundColumn> key;

    private BoundAgg(Agg.Function function, BoundColumn value, Optional<BoundColumn> key) {
      this.function = function;
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

    List<Object> readGroups(int rowId) {
      List<Object> values = new ArrayList<>(groups.size());
      for (int index = 0; index < groups.size(); index++) {
        values.add(read(groups.get(index), groupTypes.get(index), rowId));
      }
      return values;
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
    private final List<Object> groupValues;
    private final List<AggState> aggs;

    private GroupState(List<Object> groupValues, List<BoundAgg> boundAggs) {
      this.groupValues = new ArrayList<>(groupValues);
      this.aggs = new ArrayList<>(boundAggs.size());
      for (BoundAgg agg : boundAggs) {
        aggs.add(new AggState(agg.function));
      }
    }

    void update(EvaluatedBatch batch, int rowId) {
      for (int index = 0; index < aggs.size(); index++) {
        aggs.get(index).update(batch.aggs.get(index), rowId);
      }
    }

    List<Object> finish() {
      List<Object> row = new ArrayList<>(groupValues.size() + aggs.size());
      row.addAll(groupValues);
      for (AggState agg : aggs) {
        row.add(agg.result);
      }
      return row;
    }
  }

  private static final class AggState {
    private final Agg.Function function;
    private Object result;
    private Object orderingValue;

    private AggState(Agg.Function function) {
      this.function = function;
    }

    void update(EvaluatedAgg agg, int rowId) {
      Object candidate = read(agg.values, agg.valueType, rowId);
      if (candidate == null) {
        return;
      }
      Object ordering = read(agg.keys, agg.keyType, rowId);
      if (ordering == null) {
        return;
      }
      if (orderingValue == null) {
        orderingValue = ordering;
        result = candidate;
        return;
      }
      int order = compare(agg.keyType, ordering, orderingValue);
      boolean minimum = function == Agg.Function.MIN || function == Agg.Function.MIN_NON_NULL_BY;
      if ((minimum && order < 0) || (!minimum && order > 0)) {
        orderingValue = ordering;
        result = candidate;
      }
    }
  }

  private static final class GroupKey {
    private final List<Object> values;

    private GroupKey(List<Object> values) {
      this.values = Collections.unmodifiableList(new ArrayList<>(values));
    }

    static GroupKey from(List<Object> values, List<DataType> types) {
      List<Object> canonical = new ArrayList<>(values.size());
      for (int index = 0; index < values.size(); index++) {
        canonical.add(canonicalize(values.get(index), types.get(index)));
      }
      return new GroupKey(canonical);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof GroupKey && values.equals(((GroupKey) other).values);
    }

    @Override
    public int hashCode() {
      return values.hashCode();
    }
  }
}
