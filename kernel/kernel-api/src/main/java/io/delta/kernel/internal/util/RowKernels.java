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
package io.delta.kernel.internal.util;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.data.RowBackedColumnVector;
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
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Shared value operations for retained rows and borrowed column vectors. */
public final class RowKernels {
  private RowKernels() {}

  /** Copies one vector value into standalone Kernel data. */
  public static Object materialize(ColumnVector vector, DataType type, int rowId) {
    requireNonNull(vector, "vector is null");
    requireNonNull(type, "type is null");
    if (vector.isNullAt(rowId)) {
      return null;
    }
    if (vector instanceof RowBackedColumnVector) {
      return ((RowBackedColumnVector) vector).retainValue(rowId);
    }
    if (type instanceof BinaryType) {
      return vector.getBinary(rowId).clone();
    }
    if (type instanceof StructType) {
      StructType struct = (StructType) type;
      Object[] values = new Object[struct.length()];
      for (int ordinal = 0; ordinal < struct.length(); ordinal++) {
        values[ordinal] =
            materialize(vector.getChild(ordinal), struct.at(ordinal).getDataType(), rowId);
      }
      return GenericRow.fromOwnedValues(struct, values);
    }
    if (type instanceof ArrayType) {
      return materializeArray(vector.getArray(rowId), (ArrayType) type);
    }
    if (type instanceof MapType) {
      return materializeMap(vector.getMap(rowId), (MapType) type);
    }
    requireScalar(type, "materialization");
    return VectorUtils.getValueAsObject(vector, type, rowId);
  }

  /** Copies a row and all nested values into a standalone {@link GenericRow}. */
  public static Row materialize(Row row) {
    requireNonNull(row, "row is null");
    StructType schema = row.getSchema();
    Object[] values = new Object[schema.length()];
    for (int ordinal = 0; ordinal < schema.length(); ordinal++) {
      values[ordinal] = materialize(row, schema.at(ordinal).getDataType(), ordinal);
    }
    return GenericRow.fromOwnedValues(schema, values);
  }

  /** Compares a non-null vector value with a non-null retained scalar value. */
  public static int compare(DataType type, ColumnVector left, int rowId, Object right) {
    requireNonNull(type, "type is null");
    requireNonNull(left, "left vector is null");
    requireNonNull(right, "right value is null");
    if (left.isNullAt(rowId)) {
      throw new IllegalArgumentException("left value is null");
    }
    requireOrderable(type);
    if (type instanceof BooleanType) {
      return Boolean.compare(left.getBoolean(rowId), (Boolean) right);
    }
    if (type instanceof ByteType) {
      return Byte.compare(left.getByte(rowId), ((Number) right).byteValue());
    }
    if (type instanceof ShortType) {
      return Short.compare(left.getShort(rowId), ((Number) right).shortValue());
    }
    if (type instanceof IntegerType || type instanceof DateType) {
      return Integer.compare(left.getInt(rowId), ((Number) right).intValue());
    }
    if (isLongBacked(type)) {
      return Long.compare(left.getLong(rowId), ((Number) right).longValue());
    }
    if (type instanceof FloatType) {
      return Float.compare(left.getFloat(rowId), ((Number) right).floatValue());
    }
    if (type instanceof DoubleType) {
      return Double.compare(left.getDouble(rowId), ((Number) right).doubleValue());
    }
    if (type instanceof DecimalType) {
      return left.getDecimal(rowId).compareTo((BigDecimal) right);
    }
    if (isStringBacked(type)) {
      return compareStrings(left.getString(rowId), (String) right);
    }
    return compareBinary(left.getBinary(rowId), (byte[]) right);
  }

  /** Compares two non-null retained scalar values. */
  public static int compare(DataType type, Object left, Object right) {
    requireNonNull(type, "type is null");
    requireNonNull(left, "left value is null");
    requireNonNull(right, "right value is null");
    requireOrderable(type);
    if (type instanceof BooleanType) {
      return Boolean.compare((Boolean) left, (Boolean) right);
    }
    if (type instanceof ByteType) {
      return Byte.compare(((Number) left).byteValue(), ((Number) right).byteValue());
    }
    if (type instanceof ShortType) {
      return Short.compare(((Number) left).shortValue(), ((Number) right).shortValue());
    }
    if (type instanceof IntegerType || type instanceof DateType) {
      return Integer.compare(((Number) left).intValue(), ((Number) right).intValue());
    }
    if (isLongBacked(type)) {
      return Long.compare(((Number) left).longValue(), ((Number) right).longValue());
    }
    if (type instanceof FloatType) {
      return Float.compare(((Number) left).floatValue(), ((Number) right).floatValue());
    }
    if (type instanceof DoubleType) {
      return Double.compare(((Number) left).doubleValue(), ((Number) right).doubleValue());
    }
    if (type instanceof DecimalType) {
      return ((BigDecimal) left).compareTo((BigDecimal) right);
    }
    if (isStringBacked(type)) {
      return compareStrings((String) left, (String) right);
    }
    return compareBinary((byte[]) left, (byte[]) right);
  }

  /** Computes the structural hash of one vector value without retaining it. */
  public static int hash(ColumnVector vector, DataType type, int rowId) {
    return hash(vector, type, rowId, true);
  }

  private static int hash(ColumnVector vector, DataType type, int rowId, boolean includeTypeHash) {
    requireNonNull(vector, "vector is null");
    requireNonNull(type, "type is null");
    if (vector.isNullAt(rowId)) {
      return 0;
    }
    if (type instanceof StructType) {
      StructType struct = (StructType) type;
      int result = includeTypeHash ? struct.hashCode() : 1;
      for (int ordinal = 0; ordinal < struct.length(); ordinal++) {
        result =
            31 * result
                + hash(
                    vector.getChild(ordinal),
                    struct.at(ordinal).getDataType(),
                    rowId,
                    includeTypeHash);
      }
      return result;
    }
    if (type instanceof ArrayType) {
      return hashArray(vector.getArray(rowId), (ArrayType) type, includeTypeHash);
    }
    if (type instanceof MapType) {
      return hashMap(vector.getMap(rowId), (MapType) type, includeTypeHash);
    }
    requireScalar(type, "hashing");
    return hashScalar(vector, type, rowId);
  }

  /** Computes the structural hash of a retained Kernel value. */
  public static int hash(Object value, DataType type) {
    requireNonNull(type, "type is null");
    if (value == null) {
      return 0;
    }
    if (type instanceof StructType) {
      return hashRow((Row) value, (StructType) type);
    }
    if (type instanceof ArrayType) {
      return hashArray((ArrayValue) value, (ArrayType) type);
    }
    if (type instanceof MapType) {
      return hashMap((MapValue) value, (MapType) type);
    }
    requireScalar(type, "hashing");
    return hashScalar(value, type);
  }

  /** Computes the structural hash of a row, including its schema. */
  public static int hash(Row row) {
    requireNonNull(row, "row is null");
    return hashRow(row, row.getSchema(), true);
  }

  /** Computes a row's value hash under its already-validated schema. */
  public static int hashValues(Row row) {
    requireNonNull(row, "row is null");
    return hashRow(row, row.getSchema(), false);
  }

  /** Tests one vector value against a retained value using null-safe structural equality. */
  public static boolean equal(ColumnVector vector, DataType type, int rowId, Object value) {
    requireNonNull(vector, "vector is null");
    requireNonNull(type, "type is null");
    boolean vectorIsNull = vector.isNullAt(rowId);
    if (vectorIsNull || value == null) {
      return vectorIsNull && value == null;
    }
    if (type instanceof StructType) {
      return equal(vector, rowId, (Row) value, (StructType) type);
    }
    if (type instanceof ArrayType) {
      return equalArrays(vector.getArray(rowId), (ArrayValue) value, (ArrayType) type);
    }
    if (type instanceof MapType) {
      return equalMaps(vector.getMap(rowId), (MapValue) value, (MapType) type);
    }
    requireScalar(type, "equality");
    return equalScalar(vector, type, rowId, value);
  }

  /** Tests retained values using null-safe structural equality. */
  public static boolean equal(Object left, Object right, DataType type) {
    requireNonNull(type, "type is null");
    if (left == null || right == null) {
      return left == right;
    }
    if (type instanceof StructType) {
      return equalRows((Row) left, (Row) right, (StructType) type);
    }
    if (type instanceof ArrayType) {
      return equalArrays((ArrayValue) left, (ArrayValue) right, (ArrayType) type);
    }
    if (type instanceof MapType) {
      return equalMaps((MapValue) left, (MapValue) right, (MapType) type);
    }
    requireScalar(type, "equality");
    return equalScalar(left, right, type);
  }

  /** Tests rows using null-safe structural equality, including their schemas. */
  public static boolean equal(Row left, Row right) {
    requireNonNull(left, "left row is null");
    requireNonNull(right, "right row is null");
    StructType schema = left.getSchema();
    return schema.equals(right.getSchema()) && equalRows(left, right, schema, true);
  }

  /** Tests row values under the left row's already-validated schema. */
  public static boolean equalValues(Row left, Row right) {
    requireNonNull(left, "left row is null");
    requireNonNull(right, "right row is null");
    return equalRows(left, right, left.getSchema(), false);
  }

  /** Copies one row value into standalone data, bypassing native-retention hooks. */
  public static Object materialize(Row row, DataType type, int ordinal) {
    if (row.isNullAt(ordinal)) {
      return null;
    }
    if (type instanceof BinaryType) {
      return row.getBinary(ordinal).clone();
    }
    if (type instanceof StructType) {
      return materialize(row.getStruct(ordinal));
    }
    if (type instanceof ArrayType) {
      return materializeArray(row.getArray(ordinal), (ArrayType) type);
    }
    if (type instanceof MapType) {
      return materializeMap(row.getMap(ordinal), (MapType) type);
    }
    requireScalar(type, "materialization");
    return getScalar(row, type, ordinal);
  }

  private static ArrayValue materializeArray(ArrayValue array, ArrayType type) {
    ColumnVector elements = array.getElements();
    List<Object> values = new ArrayList<>(array.getSize());
    for (int index = 0; index < array.getSize(); index++) {
      values.add(materialize(elements, type.getElementType(), index));
    }
    return VectorUtils.buildArrayValue(values, type.getElementType());
  }

  private static MapValue materializeMap(MapValue map, MapType type) {
    ColumnVector keys = map.getKeys();
    ColumnVector values = map.getValues();
    List<Object> ownedKeys = new ArrayList<>(map.getSize());
    List<Object> ownedValues = new ArrayList<>(map.getSize());
    for (int index = 0; index < map.getSize(); index++) {
      ownedKeys.add(materialize(keys, type.getKeyType(), index));
      ownedValues.add(materialize(values, type.getValueType(), index));
    }
    return VectorUtils.buildMapValue(ownedKeys, ownedValues, type);
  }

  private static int hashRow(Row row, StructType type) {
    return hashRow(row, type, true);
  }

  private static int hashRow(Row row, StructType type, boolean includeTypeHash) {
    if (includeTypeHash && !type.equals(row.getSchema())) {
      throw new IllegalArgumentException("Row schema does not match type " + type);
    }
    int result = includeTypeHash ? type.hashCode() : 1;
    for (int ordinal = 0; ordinal < type.length(); ordinal++) {
      DataType fieldType = type.at(ordinal).getDataType();
      result = 31 * result + hash(row, fieldType, ordinal, includeTypeHash);
    }
    return result;
  }

  private static int hash(Row row, DataType type, int ordinal) {
    return hash(row, type, ordinal, true);
  }

  private static int hash(Row row, DataType type, int ordinal, boolean includeTypeHash) {
    if (row.isNullAt(ordinal)) {
      return 0;
    }
    if (type instanceof StructType) {
      return hashRow(row.getStruct(ordinal), (StructType) type, includeTypeHash);
    }
    if (type instanceof ArrayType) {
      return hashArray(row.getArray(ordinal), (ArrayType) type, includeTypeHash);
    }
    if (type instanceof MapType) {
      return hashMap(row.getMap(ordinal), (MapType) type, includeTypeHash);
    }
    requireScalar(type, "hashing");
    return hashScalar(row, type, ordinal);
  }

  private static int hashArray(ArrayValue array, ArrayType type) {
    return hashArray(array, type, true);
  }

  private static int hashArray(ArrayValue array, ArrayType type, boolean includeTypeHash) {
    int result = 31 * (includeTypeHash ? type.hashCode() : 1) + array.getSize();
    ColumnVector elements = array.getElements();
    for (int index = 0; index < array.getSize(); index++) {
      result = 31 * result + hash(elements, type.getElementType(), index, includeTypeHash);
    }
    return result;
  }

  private static int hashMap(MapValue map, MapType type) {
    return hashMap(map, type, true);
  }

  private static int hashMap(MapValue map, MapType type, boolean includeTypeHash) {
    int result = 31 * (includeTypeHash ? type.hashCode() : 1) + map.getSize();
    ColumnVector keys = map.getKeys();
    ColumnVector values = map.getValues();
    for (int index = 0; index < map.getSize(); index++) {
      result = 31 * result + hash(keys, type.getKeyType(), index, includeTypeHash);
      result = 31 * result + hash(values, type.getValueType(), index, includeTypeHash);
    }
    return result;
  }

  private static int hashScalar(ColumnVector vector, DataType type, int rowId) {
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
    if (isLongBacked(type)) {
      return Long.hashCode(vector.getLong(rowId));
    }
    if (type instanceof FloatType) {
      return normalizedFloatHash(vector.getFloat(rowId));
    }
    if (type instanceof DoubleType) {
      return normalizedDoubleHash(vector.getDouble(rowId));
    }
    if (type instanceof DecimalType) {
      return vector.getDecimal(rowId).stripTrailingZeros().hashCode();
    }
    if (isStringBacked(type)) {
      return vector.getString(rowId).hashCode();
    }
    return Arrays.hashCode(vector.getBinary(rowId));
  }

  private static int hashScalar(Object value, DataType type) {
    if (type instanceof BooleanType) {
      return Boolean.hashCode((Boolean) value);
    }
    if (type instanceof ByteType) {
      return Byte.hashCode(((Number) value).byteValue());
    }
    if (type instanceof ShortType) {
      return Short.hashCode(((Number) value).shortValue());
    }
    if (type instanceof IntegerType || type instanceof DateType) {
      return Integer.hashCode(((Number) value).intValue());
    }
    if (isLongBacked(type)) {
      return Long.hashCode(((Number) value).longValue());
    }
    if (type instanceof FloatType) {
      return normalizedFloatHash(((Number) value).floatValue());
    }
    if (type instanceof DoubleType) {
      return normalizedDoubleHash(((Number) value).doubleValue());
    }
    if (type instanceof DecimalType) {
      return ((BigDecimal) value).stripTrailingZeros().hashCode();
    }
    if (isStringBacked(type)) {
      return value.hashCode();
    }
    return Arrays.hashCode((byte[]) value);
  }

  private static int hashScalar(Row row, DataType type, int ordinal) {
    if (type instanceof BooleanType) {
      return Boolean.hashCode(row.getBoolean(ordinal));
    }
    if (type instanceof ByteType) {
      return Byte.hashCode(row.getByte(ordinal));
    }
    if (type instanceof ShortType) {
      return Short.hashCode(row.getShort(ordinal));
    }
    if (type instanceof IntegerType || type instanceof DateType) {
      return Integer.hashCode(row.getInt(ordinal));
    }
    if (isLongBacked(type)) {
      return Long.hashCode(row.getLong(ordinal));
    }
    if (type instanceof FloatType) {
      return normalizedFloatHash(row.getFloat(ordinal));
    }
    if (type instanceof DoubleType) {
      return normalizedDoubleHash(row.getDouble(ordinal));
    }
    if (type instanceof DecimalType) {
      return row.getDecimal(ordinal).stripTrailingZeros().hashCode();
    }
    if (isStringBacked(type)) {
      return row.getString(ordinal).hashCode();
    }
    return Arrays.hashCode(row.getBinary(ordinal));
  }

  private static boolean equalRows(Row left, Row right, StructType type) {
    return equalRows(left, right, type, true);
  }

  private static boolean equalRows(Row left, Row right, StructType type, boolean validateSchema) {
    if (validateSchema && (!type.equals(left.getSchema()) || !type.equals(right.getSchema()))) {
      return false;
    }
    for (int ordinal = 0; ordinal < type.length(); ordinal++) {
      if (!equal(left, right, type.at(ordinal).getDataType(), ordinal, validateSchema)) {
        return false;
      }
    }
    return true;
  }

  private static boolean equal(Row left, Row right, DataType type, int ordinal) {
    return equal(left, right, type, ordinal, true);
  }

  private static boolean equal(
      Row left, Row right, DataType type, int ordinal, boolean validateSchema) {
    boolean leftIsNull = left.isNullAt(ordinal);
    boolean rightIsNull = right.isNullAt(ordinal);
    if (leftIsNull || rightIsNull) {
      return leftIsNull && rightIsNull;
    }
    if (type instanceof StructType) {
      return equalRows(
          left.getStruct(ordinal), right.getStruct(ordinal), (StructType) type, validateSchema);
    }
    if (type instanceof ArrayType) {
      return equalArrays(left.getArray(ordinal), right.getArray(ordinal), (ArrayType) type);
    }
    if (type instanceof MapType) {
      return equalMaps(left.getMap(ordinal), right.getMap(ordinal), (MapType) type);
    }
    requireScalar(type, "equality");
    return equalScalar(left, right, type, ordinal);
  }

  private static boolean equal(ColumnVector vector, int rowId, Row row, StructType type) {
    if (!type.equals(row.getSchema())) {
      return false;
    }
    for (int ordinal = 0; ordinal < type.length(); ordinal++) {
      DataType fieldType = type.at(ordinal).getDataType();
      if (!equal(vector.getChild(ordinal), fieldType, rowId, get(row, fieldType, ordinal))) {
        return false;
      }
    }
    return true;
  }

  private static boolean equalArrays(ArrayValue left, ArrayValue right, ArrayType type) {
    if (left.getSize() != right.getSize()) {
      return false;
    }
    return equalVectors(
        left.getElements(), right.getElements(), type.getElementType(), left.getSize());
  }

  private static boolean equalMaps(MapValue left, MapValue right, MapType type) {
    if (left.getSize() != right.getSize()) {
      return false;
    }
    return equalVectors(left.getKeys(), right.getKeys(), type.getKeyType(), left.getSize())
        && equalVectors(left.getValues(), right.getValues(), type.getValueType(), left.getSize());
  }

  private static boolean equalVectors(
      ColumnVector left, ColumnVector right, DataType type, int size) {
    for (int index = 0; index < size; index++) {
      if (!equal(left, type, index, get(right, type, index))) {
        return false;
      }
    }
    return true;
  }

  private static boolean equalScalar(ColumnVector left, DataType type, int rowId, Object right) {
    if (type instanceof BooleanType) {
      return left.getBoolean(rowId) == (Boolean) right;
    }
    if (type instanceof ByteType) {
      return left.getByte(rowId) == ((Number) right).byteValue();
    }
    if (type instanceof ShortType) {
      return left.getShort(rowId) == ((Number) right).shortValue();
    }
    if (type instanceof IntegerType || type instanceof DateType) {
      return left.getInt(rowId) == ((Number) right).intValue();
    }
    if (isLongBacked(type)) {
      return left.getLong(rowId) == ((Number) right).longValue();
    }
    if (type instanceof FloatType) {
      return equalFloats(left.getFloat(rowId), ((Number) right).floatValue());
    }
    if (type instanceof DoubleType) {
      return equalDoubles(left.getDouble(rowId), ((Number) right).doubleValue());
    }
    if (type instanceof DecimalType) {
      return left.getDecimal(rowId).compareTo((BigDecimal) right) == 0;
    }
    if (isStringBacked(type)) {
      return left.getString(rowId).equals(right);
    }
    return Arrays.equals(left.getBinary(rowId), (byte[]) right);
  }

  private static boolean equalScalar(Object left, Object right, DataType type) {
    if (type instanceof BooleanType) {
      return ((Boolean) left).booleanValue() == ((Boolean) right).booleanValue();
    }
    if (type instanceof ByteType) {
      return ((Number) left).byteValue() == ((Number) right).byteValue();
    }
    if (type instanceof ShortType) {
      return ((Number) left).shortValue() == ((Number) right).shortValue();
    }
    if (type instanceof IntegerType || type instanceof DateType) {
      return ((Number) left).intValue() == ((Number) right).intValue();
    }
    if (isLongBacked(type)) {
      return ((Number) left).longValue() == ((Number) right).longValue();
    }
    if (type instanceof FloatType) {
      return equalFloats(((Number) left).floatValue(), ((Number) right).floatValue());
    }
    if (type instanceof DoubleType) {
      return equalDoubles(((Number) left).doubleValue(), ((Number) right).doubleValue());
    }
    if (type instanceof DecimalType) {
      return ((BigDecimal) left).compareTo((BigDecimal) right) == 0;
    }
    if (type instanceof BinaryType) {
      return Arrays.equals((byte[]) left, (byte[]) right);
    }
    return ((String) left).equals(right);
  }

  private static boolean equalScalar(Row left, Row right, DataType type, int ordinal) {
    if (type instanceof BooleanType) {
      return left.getBoolean(ordinal) == right.getBoolean(ordinal);
    }
    if (type instanceof ByteType) {
      return left.getByte(ordinal) == right.getByte(ordinal);
    }
    if (type instanceof ShortType) {
      return left.getShort(ordinal) == right.getShort(ordinal);
    }
    if (type instanceof IntegerType || type instanceof DateType) {
      return left.getInt(ordinal) == right.getInt(ordinal);
    }
    if (isLongBacked(type)) {
      return left.getLong(ordinal) == right.getLong(ordinal);
    }
    if (type instanceof FloatType) {
      return equalFloats(left.getFloat(ordinal), right.getFloat(ordinal));
    }
    if (type instanceof DoubleType) {
      return equalDoubles(left.getDouble(ordinal), right.getDouble(ordinal));
    }
    if (type instanceof DecimalType) {
      return left.getDecimal(ordinal).compareTo(right.getDecimal(ordinal)) == 0;
    }
    if (isStringBacked(type)) {
      return left.getString(ordinal).equals(right.getString(ordinal));
    }
    return Arrays.equals(left.getBinary(ordinal), right.getBinary(ordinal));
  }

  private static Object get(ColumnVector vector, DataType type, int rowId) {
    if (vector.isNullAt(rowId)) {
      return null;
    }
    requireValueType(type, "equality");
    return VectorUtils.getValueAsObject(vector, type, rowId);
  }

  private static Object get(Row row, DataType type, int ordinal) {
    if (row.isNullAt(ordinal)) {
      return null;
    }
    if (type instanceof StructType) {
      return row.getStruct(ordinal);
    }
    if (type instanceof ArrayType) {
      return row.getArray(ordinal);
    }
    if (type instanceof MapType) {
      return row.getMap(ordinal);
    }
    requireScalar(type, "equality");
    return getScalar(row, type, ordinal);
  }

  private static Object getScalar(Row row, DataType type, int ordinal) {
    if (type instanceof BooleanType) {
      return row.getBoolean(ordinal);
    }
    if (type instanceof ByteType) {
      return row.getByte(ordinal);
    }
    if (type instanceof ShortType) {
      return row.getShort(ordinal);
    }
    if (type instanceof IntegerType || type instanceof DateType) {
      return row.getInt(ordinal);
    }
    if (isLongBacked(type)) {
      return row.getLong(ordinal);
    }
    if (type instanceof FloatType) {
      return row.getFloat(ordinal);
    }
    if (type instanceof DoubleType) {
      return row.getDouble(ordinal);
    }
    if (type instanceof DecimalType) {
      return row.getDecimal(ordinal);
    }
    if (isStringBacked(type)) {
      return row.getString(ordinal);
    }
    return row.getBinary(ordinal);
  }

  private static int normalizedFloatHash(float value) {
    return Float.hashCode(value == 0.0f ? 0.0f : value);
  }

  private static int normalizedDoubleHash(double value) {
    return Double.hashCode(value == 0.0d ? 0.0d : value);
  }

  private static boolean equalFloats(float left, float right) {
    return (left == 0.0f && right == 0.0f) || Float.compare(left, right) == 0;
  }

  private static boolean equalDoubles(double left, double right) {
    return (left == 0.0d && right == 0.0d) || Double.compare(left, right) == 0;
  }

  private static int compareStrings(String left, String right) {
    int leftOffset = 0;
    int rightOffset = 0;
    while (leftOffset < left.length() && rightOffset < right.length()) {
      int leftCodePoint = left.codePointAt(leftOffset);
      int rightCodePoint = right.codePointAt(rightOffset);
      if (leftCodePoint != rightCodePoint) {
        return Integer.compare(leftCodePoint, rightCodePoint);
      }
      leftOffset += Character.charCount(leftCodePoint);
      rightOffset += Character.charCount(rightCodePoint);
    }
    return Integer.compare(left.length() - leftOffset, right.length() - rightOffset);
  }

  private static int compareBinary(byte[] left, byte[] right) {
    int size = Math.min(left.length, right.length);
    for (int index = 0; index < size; index++) {
      int comparison =
          Integer.compare(Byte.toUnsignedInt(left[index]), Byte.toUnsignedInt(right[index]));
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(left.length, right.length);
  }

  /** Rejects types that do not have Kernel scalar ordering semantics. */
  public static void requireOrderable(DataType type) {
    requireNonNull(type, "type is null");
    if (!isScalar(type)) {
      throw unsupported("comparison", type);
    }
  }

  private static void requireScalar(DataType type, String operation) {
    if (!isScalar(type)) {
      throw unsupported(operation, type);
    }
  }

  private static void requireValueType(DataType type, String operation) {
    if (!isScalar(type)
        && !(type instanceof StructType)
        && !(type instanceof ArrayType)
        && !(type instanceof MapType)) {
      throw unsupported(operation, type);
    }
  }

  private static boolean isScalar(DataType type) {
    return type instanceof BooleanType
        || type instanceof ByteType
        || type instanceof ShortType
        || type instanceof IntegerType
        || type instanceof DateType
        || isLongBacked(type)
        || type instanceof FloatType
        || type instanceof DoubleType
        || type instanceof DecimalType
        || isStringBacked(type)
        || type instanceof BinaryType;
  }

  private static boolean isLongBacked(DataType type) {
    return type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType;
  }

  private static boolean isStringBacked(DataType type) {
    return type instanceof StringType
        || type instanceof GeometryType
        || type instanceof GeographyType;
  }

  private static UnsupportedOperationException unsupported(String operation, DataType type) {
    return new UnsupportedOperationException(
        "Unsupported type for Kernel value " + operation + ": " + type);
  }
}
