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

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector;
import io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.internal.plans.PlanSchemaUtils;
import io.delta.kernel.internal.plans.SemiJoin;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Executes a {@link SemiJoin} by materializing build keys and lazily filtering probe batches. */
final class SemiJoinExecutor {
  private SemiJoinExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(
      SemiJoin join,
      StructType probeSchema,
      StructType buildSchema,
      CloseableIterator<FilteredColumnarBatch> probe,
      CloseableIterator<FilteredColumnarBatch> build) {
    requireNonNull(join, "join is null");
    requireNonNull(probeSchema, "probeSchema is null");
    requireNonNull(buildSchema, "buildSchema is null");
    requireNonNull(probe, "probe is null");
    requireNonNull(build, "build is null");

    join.getOutputSchema(Arrays.asList(probeSchema, buildSchema));
    KeyProjection probeKeys = KeyProjection.bind(probeSchema, join.getProbeKeys(), "probe");
    KeyProjection buildKeys = KeyProjection.bind(buildSchema, join.getBuildKeys(), "build");
    Set<List<Object>> materialized = materializeBuild(buildSchema, buildKeys, build, probe);

    return probe.map(
        batch -> filterProbe(join.isInverted(), probeSchema, probeKeys, materialized, batch));
  }

  private static Set<List<Object>> materializeBuild(
      StructType schema,
      KeyProjection projection,
      CloseableIterator<FilteredColumnarBatch> build,
      CloseableIterator<FilteredColumnarBatch> probe) {
    Set<List<Object>> keys = new HashSet<>();
    try {
      while (build.hasNext()) {
        FilteredColumnarBatch batch = requireNonNull(build.next(), "build batch is null");
        validateSchema("build", schema, batch.getData());
        try (EvaluatedKeys values = projection.evaluate(batch.getData())) {
          for (int rowId = 0; rowId < batch.getData().getSize(); rowId++) {
            if (isSelected(batch, rowId)) {
              keys.add(values.keyAt(rowId));
            }
          }
        }
      }
    } catch (RuntimeException | Error failure) {
      closeAfterFailure(failure, build, probe);
      throw failure;
    }

    try {
      Utils.closeCloseables(build);
    } catch (RuntimeException | Error failure) {
      closeAfterFailure(failure, probe);
      throw failure;
    }
    return keys;
  }

  private static FilteredColumnarBatch filterProbe(
      boolean inverted,
      StructType schema,
      KeyProjection projection,
      Set<List<Object>> buildKeys,
      FilteredColumnarBatch batch) {
    requireNonNull(batch, "probe batch is null");
    ColumnarBatch data = batch.getData();
    validateSchema("probe", schema, data);
    boolean[] selected = new boolean[data.getSize()];
    try (EvaluatedKeys values = projection.evaluate(data)) {
      for (int rowId = 0; rowId < data.getSize(); rowId++) {
        if (isSelected(batch, rowId)) {
          selected[rowId] = inverted != buildKeys.contains(values.keyAt(rowId));
        }
      }
    }
    return batch.withSelectionVector(
        new DefaultBooleanVector(selected.length, Optional.empty(), selected));
  }

  private static void validateSchema(String side, StructType expected, ColumnarBatch data) {
    requireNonNull(data, side + " batch data is null");
    if (!expected.equals(data.getSchema())) {
      throw new IllegalArgumentException(
          "SemiJoin "
              + side
              + " batch schema "
              + data.getSchema()
              + " does not match expected schema "
              + expected);
    }
  }

  private static boolean isSelected(FilteredColumnarBatch batch, int rowId) {
    Optional<ColumnVector> selection = batch.getSelectionVector();
    return !selection.isPresent()
        || (!selection.get().isNullAt(rowId) && selection.get().getBoolean(rowId));
  }

  private static void closeAfterFailure(Throwable failure, AutoCloseable... closeables) {
    try {
      Utils.closeCloseables(closeables);
    } catch (RuntimeException | Error closeFailure) {
      failure.addSuppressed(closeFailure);
    }
  }

  private static Object canonicalValue(ColumnVector vector, DataType type, int rowId) {
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
      float value = vector.getFloat(rowId);
      return value == 0.0f ? 0.0f : (Float.isNaN(value) ? Float.NaN : value);
    } else if (type instanceof DoubleType) {
      double value = vector.getDouble(rowId);
      return value == 0.0d ? 0.0d : (Double.isNaN(value) ? Double.NaN : value);
    } else if (type instanceof DecimalType) {
      return vector.getDecimal(rowId);
    } else if (type instanceof StringType) {
      return vector.getString(rowId);
    } else if (DataType.isTypeValueBinaryLike(type)) {
      return ByteBuffer.wrap(vector.getBinary(rowId).clone()).asReadOnlyBuffer();
    } else if (type instanceof StructType) {
      StructType struct = (StructType) type;
      List<Object> fields = new ArrayList<>(struct.length());
      for (int ordinal = 0; ordinal < struct.length(); ordinal++) {
        fields.add(
            canonicalValue(vector.getChild(ordinal), struct.at(ordinal).getDataType(), rowId));
      }
      return fields;
    } else if (type instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) type;
      ArrayValue array = vector.getArray(rowId);
      List<Object> elements = new ArrayList<>(array.getSize());
      for (int index = 0; index < array.getSize(); index++) {
        elements.add(canonicalValue(array.getElements(), arrayType.getElementType(), index));
      }
      return elements;
    } else if (type instanceof MapType) {
      MapType mapType = (MapType) type;
      MapValue map = vector.getMap(rowId);
      Map<Object, Object> entries = new HashMap<>();
      for (int index = 0; index < map.getSize(); index++) {
        Object key = canonicalValue(map.getKeys(), mapType.getKeyType(), index);
        Object value = canonicalValue(map.getValues(), mapType.getValueType(), index);
        entries.put(key, value);
      }
      return entries;
    }
    throw new UnsupportedOperationException("SemiJoin key has unsupported data type " + type);
  }

  private static final class KeyProjection {
    private final List<ExpressionEvaluator> evaluators;
    private final List<DataType> types;

    private KeyProjection(List<ExpressionEvaluator> evaluators, List<DataType> types) {
      this.evaluators = evaluators;
      this.types = types;
    }

    static KeyProjection bind(StructType schema, List<Column> columns, String side) {
      List<ExpressionEvaluator> evaluators = new ArrayList<>(columns.size());
      List<DataType> types = new ArrayList<>(columns.size());
      for (Column column : columns) {
        StructField field =
            PlanSchemaUtils.resolveField(schema, column, "SemiJoin " + side + " key");
        DataType type = field.getDataType();
        validateKeyType(type);
        evaluators.add(new DefaultExpressionEvaluator(schema, column, type));
        types.add(type);
      }
      return new KeyProjection(evaluators, types);
    }

    private static void validateKeyType(DataType type) {
      if (type instanceof BooleanType
          || type instanceof ByteType
          || type instanceof ShortType
          || type instanceof IntegerType
          || type instanceof DateType
          || type instanceof LongType
          || type instanceof TimestampType
          || type instanceof TimestampNTZType
          || type instanceof FloatType
          || type instanceof DoubleType
          || type instanceof DecimalType
          || type instanceof StringType
          || DataType.isTypeValueBinaryLike(type)) {
        return;
      }
      if (type instanceof StructType) {
        for (StructField field : ((StructType) type).fields()) {
          validateKeyType(field.getDataType());
        }
        return;
      }
      if (type instanceof ArrayType) {
        validateKeyType(((ArrayType) type).getElementType());
        return;
      }
      if (type instanceof MapType) {
        MapType map = (MapType) type;
        validateKeyType(map.getKeyType());
        validateKeyType(map.getValueType());
        return;
      }
      throw new UnsupportedOperationException("SemiJoin key has unsupported data type " + type);
    }

    EvaluatedKeys evaluate(ColumnarBatch data) {
      List<ColumnVector> vectors = new ArrayList<>(evaluators.size());
      try {
        for (ExpressionEvaluator evaluator : evaluators) {
          vectors.add(evaluator.eval(data));
        }
        return new EvaluatedKeys(vectors, types);
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(failure, vectors.toArray(new AutoCloseable[0]));
        throw failure;
      }
    }
  }

  private static final class EvaluatedKeys implements AutoCloseable {
    private final List<ColumnVector> vectors;
    private final List<DataType> types;

    private EvaluatedKeys(List<ColumnVector> vectors, List<DataType> types) {
      this.vectors = vectors;
      this.types = types;
    }

    List<Object> keyAt(int rowId) {
      List<Object> key = new ArrayList<>(vectors.size());
      for (int index = 0; index < vectors.size(); index++) {
        key.add(canonicalValue(vectors.get(index), types.get(index), rowId));
      }
      return key;
    }

    @Override
    public void close() {
      Utils.closeCloseables(vectors.toArray(new AutoCloseable[0]));
    }
  }
}
