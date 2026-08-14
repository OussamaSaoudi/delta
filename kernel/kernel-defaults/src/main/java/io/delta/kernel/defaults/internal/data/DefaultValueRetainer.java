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
package io.delta.kernel.defaults.internal.data;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.defaults.internal.data.vector.RetainableColumnVector;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
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
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.types.VariantType;
import java.util.ArrayList;
import java.util.List;

/** Retains standalone Kernel values from default heap vectors with a generic copy fallback. */
public final class DefaultValueRetainer {
  private DefaultValueRetainer() {}

  public static Object retain(ColumnVector vector, DataType type, int rowId) {
    requireNonNull(vector, "vector is null");
    requireNonNull(type, "type is null");
    if (vector.isNullAt(rowId)) {
      return null;
    }
    if (vector instanceof RetainableColumnVector) {
      return ((RetainableColumnVector) vector).retainValue(rowId);
    }
    return materialize(vector, type, rowId);
  }

  public static Object retainDefaultVector(ColumnVector vector, DataType type, int rowId) {
    requireNonNull(vector, "vector is null");
    requireNonNull(type, "type is null");
    if (vector.isNullAt(rowId)) {
      return null;
    }
    if (type instanceof StructType) {
      StructType struct = (StructType) type;
      List<Object> fields = new ArrayList<>(struct.length());
      for (int ordinal = 0; ordinal < struct.length(); ordinal++) {
        fields.add(retain(vector.getChild(ordinal), struct.at(ordinal).getDataType(), rowId));
      }
      return GenericRow.fromValues(struct, fields);
    } else if (type instanceof ArrayType) {
      return materialize(vector, type, rowId);
    } else if (type instanceof MapType) {
      return materialize(vector, type, rowId);
    } else if (type instanceof BinaryType) {
      return vector.getBinary(rowId).clone();
    }
    return readScalar(vector, type, rowId);
  }

  /** Copies one value into generic standalone Kernel data. */
  public static Object materialize(ColumnVector vector, DataType type, int rowId) {
    requireNonNull(vector, "vector is null");
    requireNonNull(type, "type is null");
    if (vector.isNullAt(rowId)) {
      return null;
    }
    if (type instanceof BinaryType) {
      return vector.getBinary(rowId).clone();
    } else if (type instanceof StructType) {
      StructType struct = (StructType) type;
      List<Object> fields = new ArrayList<>(struct.length());
      for (int ordinal = 0; ordinal < struct.length(); ordinal++) {
        fields.add(materialize(vector.getChild(ordinal), struct.at(ordinal).getDataType(), rowId));
      }
      return GenericRow.fromValues(struct, fields);
    } else if (type instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) type;
      ArrayValue array = vector.getArray(rowId);
      ColumnVector elementsVector = array.getElements();
      List<Object> elements = new ArrayList<>(array.getSize());
      for (int index = 0; index < array.getSize(); index++) {
        elements.add(materialize(elementsVector, arrayType.getElementType(), index));
      }
      return VectorUtils.buildArrayValue(elements, arrayType.getElementType());
    } else if (type instanceof MapType) {
      MapType mapType = (MapType) type;
      MapValue map = vector.getMap(rowId);
      ColumnVector keysVector = map.getKeys();
      ColumnVector valuesVector = map.getValues();
      List<Object> keys = new ArrayList<>(map.getSize());
      List<Object> values = new ArrayList<>(map.getSize());
      for (int index = 0; index < map.getSize(); index++) {
        keys.add(materialize(keysVector, mapType.getKeyType(), index));
        values.add(materialize(valuesVector, mapType.getValueType(), index));
      }
      return VectorUtils.buildMapValue(keys, values, mapType);
    }
    return readScalar(vector, type, rowId);
  }

  private static Object readScalar(ColumnVector vector, DataType type, int rowId) {
    if (type instanceof BooleanType) {
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
    } else if (type instanceof VariantType) {
      return vector.getVariant(rowId);
    }
    throw unsupported(type);
  }

  private static UnsupportedOperationException unsupported(DataType type) {
    return new UnsupportedOperationException("Cannot retain data type " + type);
  }
}
