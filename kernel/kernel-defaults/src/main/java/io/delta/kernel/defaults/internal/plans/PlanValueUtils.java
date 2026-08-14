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

import static io.delta.kernel.defaults.internal.expressions.DefaultValueComparator.supports;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultValueRetainer;
import io.delta.kernel.internal.data.StructRow;
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
import io.delta.kernel.types.IntervalDayTimeType;
import io.delta.kernel.types.IntervalYearMonthType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.types.VariantType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Typed Kernel value access and structural keys shared by default plan operators. */
final class PlanValueUtils {
  private PlanValueUtils() {}

  /** Reads one value without changing its Kernel representation. */
  static Object read(ColumnVector vector, DataType type, int rowId) {
    requireNonNull(vector, "vector is null");
    requireNonNull(type, "type is null");
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
    } else if (type instanceof IntervalYearMonthType) {
      return vector.getIntervalYearMonth(rowId);
    } else if (type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType) {
      return vector.getLong(rowId);
    } else if (type instanceof IntervalDayTimeType) {
      return vector.getIntervalDayTime(rowId);
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
    } else if (type instanceof VariantType) {
      return vector.getVariant(rowId);
    } else if (type instanceof StructType) {
      return StructRow.fromStructVector(vector, rowId);
    } else if (type instanceof ArrayType) {
      return vector.getArray(rowId);
    } else if (type instanceof MapType) {
      return vector.getMap(rowId);
    }
    throw unsupported(type);
  }

  /** Copies one vector value into standalone Kernel data safe to retain after the vector closes. */
  static Object materialize(ColumnVector vector, DataType type, int rowId) {
    return DefaultValueRetainer.materialize(vector, type, rowId);
  }

  /** Reads one row value, cloning mutable binary data for safe retention. */
  static Object read(Row row, DataType type, int ordinal) {
    requireNonNull(row, "row is null");
    requireNonNull(type, "type is null");
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
    } else if (type instanceof IntervalYearMonthType) {
      return row.getIntervalYearMonth(ordinal);
    } else if (type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType) {
      return row.getLong(ordinal);
    } else if (type instanceof IntervalDayTimeType) {
      return row.getIntervalDayTime(ordinal);
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
      return row.getBinary(ordinal).clone();
    } else if (type instanceof VariantType) {
      return row.getVariant(ordinal);
    } else if (type instanceof StructType) {
      return row.getStruct(ordinal);
    } else if (type instanceof ArrayType) {
      return row.getArray(ordinal);
    } else if (type instanceof MapType) {
      return row.getMap(ordinal);
    }
    throw unsupported(type);
  }

  /** Returns a stable equality/hash key for one vector value. */
  static Object canonicalize(ColumnVector vector, DataType type, int rowId) {
    requireNonNull(vector, "vector is null");
    requireNonNull(type, "type is null");
    if (vector.isNullAt(rowId)) {
      return null;
    }
    if (type instanceof BinaryType) {
      return binaryKey(vector.getBinary(rowId));
    }
    if (type instanceof StructType) {
      StructType struct = (StructType) type;
      List<Object> fields = new ArrayList<>(struct.length());
      for (int ordinal = 0; ordinal < struct.length(); ordinal++) {
        fields.add(canonicalize(vector.getChild(ordinal), struct.at(ordinal).getDataType(), rowId));
      }
      return Collections.unmodifiableList(fields);
    }
    if (type instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) type;
      ArrayValue array = vector.getArray(rowId);
      List<Object> elements = new ArrayList<>(array.getSize());
      for (int index = 0; index < array.getSize(); index++) {
        elements.add(canonicalize(array.getElements(), arrayType.getElementType(), index));
      }
      return Collections.unmodifiableList(elements);
    }
    if (type instanceof MapType) {
      MapType mapType = (MapType) type;
      MapValue map = vector.getMap(rowId);
      Map<Object, Object> entries = new LinkedHashMap<>();
      for (int index = 0; index < map.getSize(); index++) {
        Object key = canonicalize(map.getKeys(), mapType.getKeyType(), index);
        Object value = canonicalize(map.getValues(), mapType.getValueType(), index);
        entries.put(key, value);
      }
      return Collections.unmodifiableMap(entries);
    }
    return normalizeScalar(read(vector, type, rowId), type);
  }

  static void validateMaterializable(DataType type, String context) {
    validate(type, context, true);
  }

  static void validateCanonicalizable(DataType type, String context, boolean allowMaps) {
    validate(type, context, allowMaps);
  }

  private static void validate(DataType type, String context, boolean allowMaps) {
    requireNonNull(type, "type is null");
    requireNonNull(context, "context is null");
    if (supports(type) || type instanceof VariantType) {
      return;
    }
    if (type instanceof StructType) {
      for (StructField field : ((StructType) type).fields()) {
        validate(field.getDataType(), context, allowMaps);
      }
      return;
    }
    if (type instanceof ArrayType) {
      validate(((ArrayType) type).getElementType(), context, allowMaps);
      return;
    }
    if (type instanceof MapType && allowMaps) {
      MapType map = (MapType) type;
      validate(map.getKeyType(), context, true);
      validate(map.getValueType(), context, true);
      return;
    }
    throw new UnsupportedOperationException(context + " has unsupported data type " + type);
  }

  private static Object normalizeScalar(Object value, DataType type) {
    if (type instanceof FloatType) {
      float number = (Float) value;
      return number == 0.0f ? 0.0f : (Float.isNaN(number) ? Float.NaN : number);
    }
    if (type instanceof DoubleType) {
      double number = (Double) value;
      return number == 0.0d ? 0.0d : (Double.isNaN(number) ? Double.NaN : number);
    }
    return value;
  }

  private static ByteBuffer binaryKey(byte[] value) {
    return ByteBuffer.wrap(requireNonNull(value, "binary value is null").clone())
        .asReadOnlyBuffer();
  }

  private static UnsupportedOperationException unsupported(DataType type) {
    return new UnsupportedOperationException("Plan cannot materialize data type " + type);
  }
}
