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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.defaults.internal.data.DefaultValueRetainer;
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
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Stable Kernel values used by materializing plan operators. */
final class PlanValueUtils {
  private PlanValueUtils() {}

  static Object materialize(ColumnVector vector, DataType type, int rowId) {
    return DefaultValueRetainer.retain(vector, type, rowId);
  }

  static int hash(ColumnVector vector, DataType type, int rowId) {
    if (vector.isNullAt(rowId)) {
      return 0;
    }
    if (type instanceof BooleanType) {
      return Boolean.hashCode(vector.getBoolean(rowId));
    }
    if (type instanceof ByteType) {
      return Byte.hashCode(vector.getByte(rowId));
    }
    if (type instanceof ShortType) {
      return Short.hashCode(vector.getShort(rowId));
    }
    if (type instanceof IntegerType || type instanceof DateType) {
      return Integer.hashCode(vector.getInt(rowId));
    }
    if (type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType) {
      return Long.hashCode(vector.getLong(rowId));
    }
    if (type instanceof FloatType) {
      float value = vector.getFloat(rowId);
      return Float.hashCode(value == 0.0f ? 0.0f : value);
    }
    if (type instanceof DoubleType) {
      double value = vector.getDouble(rowId);
      return Double.hashCode(value == 0.0d ? 0.0d : value);
    }
    if (type instanceof DecimalType) {
      return vector.getDecimal(rowId).stripTrailingZeros().hashCode();
    }
    if (type instanceof StringType
        || type instanceof GeometryType
        || type instanceof GeographyType) {
      return vector.getString(rowId).hashCode();
    }
    if (type instanceof BinaryType) {
      return Arrays.hashCode(vector.getBinary(rowId));
    }
    throw unsupported(type);
  }

  static boolean equal(ColumnVector vector, DataType type, int rowId, Object value) {
    if (vector.isNullAt(rowId) || value == null) {
      return vector.isNullAt(rowId) && value == null;
    }
    if (type instanceof BooleanType) {
      return vector.getBoolean(rowId) == (Boolean) value;
    }
    if (type instanceof ByteType) {
      return vector.getByte(rowId) == ((Number) value).byteValue();
    }
    if (type instanceof ShortType) {
      return vector.getShort(rowId) == ((Number) value).shortValue();
    }
    if (type instanceof IntegerType || type instanceof DateType) {
      return vector.getInt(rowId) == ((Number) value).intValue();
    }
    if (type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType) {
      return vector.getLong(rowId) == ((Number) value).longValue();
    }
    if (type instanceof FloatType) {
      float left = vector.getFloat(rowId);
      float right = (Float) value;
      return (left == 0.0f && right == 0.0f) || Float.compare(left, right) == 0;
    }
    if (type instanceof DoubleType) {
      double left = vector.getDouble(rowId);
      double right = (Double) value;
      return (left == 0.0d && right == 0.0d) || Double.compare(left, right) == 0;
    }
    if (type instanceof DecimalType) {
      return vector.getDecimal(rowId).compareTo((java.math.BigDecimal) value) == 0;
    }
    if (type instanceof StringType
        || type instanceof GeometryType
        || type instanceof GeographyType) {
      return vector.getString(rowId).equals(value);
    }
    if (type instanceof BinaryType) {
      return Arrays.equals(vector.getBinary(rowId), (byte[]) value);
    }
    throw unsupported(type);
  }

  static boolean equal(Object left, Object right, DataType type) {
    if (left == null || right == null) {
      return left == right;
    }
    if (type instanceof FloatType && ((Float) left) == 0.0f && ((Float) right) == 0.0f) {
      return true;
    }
    if (type instanceof DoubleType && ((Double) left) == 0.0d && ((Double) right) == 0.0d) {
      return true;
    }
    if (type instanceof BinaryType) {
      return Arrays.equals((byte[]) left, (byte[]) right);
    }
    return compare(type, left, right) == 0;
  }

  static void requireGroupType(DataType type) {
    if (!supportsComparison(type)) {
      throw unsupported(type);
    }
  }

  /** Hash table probed directly from vectors and materialized only when inserting a new key. */
  static final class KeyMap<T> {
    private final List<DataType> types;
    private final Map<Key, T> values = new HashMap<>();
    private final Probe probe = new Probe();

    KeyMap(List<DataType> types) {
      this.types = Collections.unmodifiableList(new ArrayList<>(types));
    }

    T getOrInsert(
        List<ColumnVector> vectors, int rowId, Function<Object[], T> createValue) {
      probe.reset(vectors, rowId);
      T existing = values.get(probe);
      if (existing != null) {
        return existing;
      }
      Key key = probe.materialize();
      T inserted = createValue.apply(key.values);
      values.put(key, inserted);
      return inserted;
    }

    boolean contains(List<ColumnVector> vectors, int rowId) {
      probe.reset(vectors, rowId);
      return values.containsKey(probe);
    }

    Collection<T> values() {
      return values.values();
    }

    int size() {
      return values.size();
    }

    private final class Probe {
      private List<ColumnVector> vectors;
      private int rowId;
      private int hashCode;

      private void reset(List<ColumnVector> vectors, int rowId) {
        this.vectors = vectors;
        this.rowId = rowId;
        hashCode = 1;
        for (int index = 0; index < types.size(); index++) {
          hashCode = 31 * hashCode + hash(vectors.get(index), types.get(index), rowId);
        }
      }

      private Key materialize() {
        Object[] keyValues = new Object[types.size()];
        for (int index = 0; index < types.size(); index++) {
          keyValues[index] =
              PlanValueUtils.materialize(vectors.get(index), types.get(index), rowId);
        }
        return new Key(types, keyValues, hashCode);
      }

      @Override
      public int hashCode() {
        return hashCode;
      }

      @Override
      public boolean equals(Object other) {
        if (!(other instanceof Key)) {
          return false;
        }
        Key key = (Key) other;
        for (int index = 0; index < types.size(); index++) {
          if (!equal(vectors.get(index), types.get(index), rowId, key.values[index])) {
            return false;
          }
        }
        return true;
      }
    }

    private static final class Key {
      private final List<DataType> types;
      private final Object[] values;
      private final int hashCode;

      private Key(List<DataType> types, Object[] values, int hashCode) {
        this.types = types;
        this.values = values;
        this.hashCode = hashCode;
      }

      @Override
      public int hashCode() {
        return hashCode;
      }

      @Override
      public boolean equals(Object other) {
        if (!(other instanceof Key)) {
          return false;
        }
        Key key = (Key) other;
        for (int index = 0; index < types.size(); index++) {
          if (!equal(values[index], key.values[index], types.get(index))) {
            return false;
          }
        }
        return true;
      }
    }
  }

  private static UnsupportedOperationException unsupported(DataType type) {
    return new UnsupportedOperationException("Unsupported plan key type " + type);
  }

}
