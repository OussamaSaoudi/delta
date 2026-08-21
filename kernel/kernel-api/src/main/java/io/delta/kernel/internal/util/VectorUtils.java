/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.util;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericColumnVector;
import io.delta.kernel.internal.data.StructRow;
import io.delta.kernel.types.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class VectorUtils {

  private VectorUtils() {}

  /**
   * Converts an {@link ArrayValue} to a Java list. Any nested complex types are also converted to
   * their Java type.
   */
  public static <T> List<T> toJavaList(ArrayValue arrayValue) {
    final ColumnVector elementVector = arrayValue.getElements();
    final DataType dataType = elementVector.getDataType();

    List<T> elements = new ArrayList<>();
    for (int i = 0; i < arrayValue.getSize(); i++) {
      elements.add((T) getValueAsJavaObject(elementVector, dataType, i));
    }
    return elements;
  }

  /**
   * Converts a {@link MapValue} to a Java map. Any nested complex types are also converted to their
   * Java type.
   *
   * <p>Please note not all key types override hashCode/equals. Be careful when using with keys of:
   * - Struct type at any nesting level (i.e. ArrayType(StructType) does not) - Binary type
   */
  public static <K, V> Map<K, V> toJavaMap(MapValue mapValue) {
    final ColumnVector keyVector = mapValue.getKeys();
    final DataType keyDataType = keyVector.getDataType();
    final ColumnVector valueVector = mapValue.getValues();
    final DataType valueDataType = valueVector.getDataType();

    Map<K, V> values = new HashMap<>();

    for (int i = 0; i < mapValue.getSize(); i++) {
      Object key = getValueAsJavaObject(keyVector, keyDataType, i);
      Object value = getValueAsJavaObject(valueVector, valueDataType, i);
      values.put((K) key, (V) value);
    }
    return values;
  }

  /**
   * Creates a {@link MapValue} from map of string keys and string values. The type {@code
   * map(string -> string)} is a common occurrence in Delta Log schema.
   *
   * @param keyValues
   * @return
   */
  public static MapValue stringStringMapValue(Map<String, String> keyValues) {
    List<String> keys = new ArrayList<>();
    List<String> values = new ArrayList<>();
    for (Map.Entry<String, String> entry : keyValues.entrySet()) {
      keys.add(entry.getKey());
      values.add(entry.getValue());
    }
    return buildMapValue(
        keys,
        values,
        new MapType(StringType.STRING, StringType.STRING, true /* valueContainsNull */));
  }

  /** Creates an {@link ArrayValue} from list of objects. */
  public static ArrayValue buildArrayValue(List<?> values, DataType dataType) {
    if (values == null) {
      return null;
    }
    return new ArrayValue() {
      @Override
      public int getSize() {
        return values.size();
      }

      @Override
      public ColumnVector getElements() {
        return buildColumnVector(values, dataType);
      }
    };
  }

  /** Creates a {@link MapValue} from key and value lists in entry order. */
  public static MapValue buildMapValue(List<?> keys, List<?> values, MapType dataType) {
    if (keys == null || values == null) {
      return null;
    }
    if (keys.size() != values.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Map keys and values have different sizes: %s != %s", keys.size(), values.size()));
    }
    return new MapValue() {
      @Override
      public int getSize() {
        return keys.size();
      }

      @Override
      public ColumnVector getKeys() {
        return buildColumnVector(keys, dataType.getKeyType());
      }

      @Override
      public ColumnVector getValues() {
        return buildColumnVector(values, dataType.getValueType());
      }
    };
  }

  /**
   * Utility method to create a {@link ColumnVector} for given list of object, the object should be
   * primitive type or an Row instance.
   *
   * @param values list of strings
   * @return a {@link ColumnVector} with the given values type.
   */
  public static ColumnVector buildColumnVector(List<?> values, DataType dataType) {
    return new GenericColumnVector(values, dataType);
  }

  /**
   * Gets the Kernel-native value at {@code rowId}. Complex types use the same representations as
   * {@link Row}: {@link Row}, {@link ArrayValue}, and {@link MapValue}.
   */
  public static Object getValueAsObject(ColumnVector columnVector, DataType dataType, int rowId) {
    if (columnVector.isNullAt(rowId)) {
      return null;
    } else if (dataType instanceof BooleanType) {
      return columnVector.getBoolean(rowId);
    } else if (dataType instanceof ByteType) {
      return columnVector.getByte(rowId);
    } else if (dataType instanceof ShortType) {
      return columnVector.getShort(rowId);
    } else if (dataType instanceof IntegerType || dataType instanceof DateType) {
      // DateType data is stored internally as the number of days since 1970-01-01
      return columnVector.getInt(rowId);
    } else if (dataType instanceof LongType
        || dataType instanceof TimestampType
        || dataType instanceof TimestampNTZType) {
      // TimestampType data is stored internally as the number of microseconds since the unix
      // epoch
      return columnVector.getLong(rowId);
    } else if (dataType instanceof FloatType) {
      return columnVector.getFloat(rowId);
    } else if (dataType instanceof DoubleType) {
      return columnVector.getDouble(rowId);
    } else if (dataType instanceof StringType
        || dataType instanceof GeometryType
        || dataType instanceof GeographyType) {
      return columnVector.getString(rowId);
    } else if (dataType instanceof BinaryType) {
      return columnVector.getBinary(rowId);
    } else if (dataType instanceof StructType) {
      // TODO are we okay with this usage of StructRow?
      return StructRow.fromStructVector(columnVector, rowId);
    } else if (dataType instanceof DecimalType) {
      return columnVector.getDecimal(rowId);
    } else if (dataType instanceof ArrayType) {
      return columnVector.getArray(rowId);
    } else if (dataType instanceof MapType) {
      return columnVector.getMap(rowId);
    } else {
      throw new UnsupportedOperationException("unsupported data type");
    }
  }

  private static Object getValueAsJavaObject(
      ColumnVector columnVector, DataType dataType, int rowId) {
    Object value = getValueAsObject(columnVector, dataType, rowId);
    if (value instanceof ArrayValue) {
      return toJavaList((ArrayValue) value);
    }
    if (value instanceof MapValue) {
      return toJavaMap((MapValue) value);
    }
    return value;
  }
}
