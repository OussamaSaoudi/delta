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

import io.delta.kernel.data.Row;
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
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.math.BigDecimal;
import java.util.Arrays;

/** Hash, equality, and ordering for scalar and struct execution values. */
public final class RowKernels {
  private RowKernels() {}

  /** Compares two non-null row values. */
  public static int compare(
      DataType type, Row left, int leftOrdinal, Row right, int rightOrdinal) {
    requireNonNull(type, "type is null");
    requireNonNull(left, "left row is null");
    requireNonNull(right, "right row is null");
    if (left.isNullAt(leftOrdinal) || right.isNullAt(rightOrdinal)) {
      throw new IllegalArgumentException("compared value is null");
    }
    requireOrderable(type);
    if (type instanceof BooleanType) {
      return Boolean.compare(left.getBoolean(leftOrdinal), right.getBoolean(rightOrdinal));
    }
    if (type instanceof ByteType) {
      return Byte.compare(left.getByte(leftOrdinal), right.getByte(rightOrdinal));
    }
    if (type instanceof ShortType) {
      return Short.compare(left.getShort(leftOrdinal), right.getShort(rightOrdinal));
    }
    if (type instanceof IntegerType || type instanceof DateType) {
      return Integer.compare(left.getInt(leftOrdinal), right.getInt(rightOrdinal));
    }
    if (isLongBacked(type)) {
      return Long.compare(left.getLong(leftOrdinal), right.getLong(rightOrdinal));
    }
    if (type instanceof FloatType) {
      return Float.compare(left.getFloat(leftOrdinal), right.getFloat(rightOrdinal));
    }
    if (type instanceof DoubleType) {
      return Double.compare(left.getDouble(leftOrdinal), right.getDouble(rightOrdinal));
    }
    if (type instanceof DecimalType) {
      return left.getDecimal(leftOrdinal).compareTo(right.getDecimal(rightOrdinal));
    }
    if (isStringBacked(type)) {
      return compareStrings(left.getString(leftOrdinal), right.getString(rightOrdinal));
    }
    return compareBinary(left.getBinary(leftOrdinal), right.getBinary(rightOrdinal));
  }

  /** Computes a row's structural hash, including its schema. */
  public static int hash(Row row) {
    requireNonNull(row, "row is null");
    return hashRow(row, requireNonNull(row.getSchema(), "row schema is null"), true);
  }

  /** Computes a row prefix's value hash. */
  public static int hashValues(Row row, StructType schema) {
    requireNonNull(row, "row is null");
    requireNonNull(schema, "schema is null");
    int result = 1;
    for (int ordinal = 0; ordinal < schema.length(); ordinal++) {
      result = 31 * result + hash(row, schema.at(ordinal).getDataType(), ordinal, false);
    }
    return result;
  }

  /** Tests rows using null-safe structural equality, including their schemas. */
  public static boolean equal(Row left, Row right) {
    requireNonNull(left, "left row is null");
    requireNonNull(right, "right row is null");
    StructType schema = requireNonNull(left.getSchema(), "left row schema is null");
    return schema.equals(right.getSchema()) && equalRows(left, right, schema, true);
  }

  /** Tests row prefixes under an already-validated state schema. */
  public static boolean equalValues(Row left, StructType schema, Row right) {
    requireNonNull(left, "left row is null");
    requireNonNull(right, "right row is null");
    requireNonNull(schema, "schema is null");
    if (!schema.equals(right.getSchema())) {
      return false;
    }
    for (int ordinal = 0; ordinal < schema.length(); ordinal++) {
      if (!equal(left, right, schema.at(ordinal).getDataType(), ordinal, false)) {
        return false;
      }
    }
    return true;
  }

  /** Rejects collection-bearing types that cannot be used as execution hash keys. */
  public static void requireHashable(DataType type) {
    requireNonNull(type, "type is null");
    if (type instanceof StructType) {
      StructType struct = (StructType) type;
      for (int ordinal = 0; ordinal < struct.length(); ordinal++) {
        requireHashable(struct.at(ordinal).getDataType());
      }
      return;
    }
    requireScalar(type, "hashing");
  }

  /** Rejects types that do not have Kernel scalar ordering semantics. */
  public static void requireOrderable(DataType type) {
    requireNonNull(type, "type is null");
    requireScalar(type, "comparison");
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

  private static int hash(Row row, DataType type, int ordinal, boolean includeTypeHash) {
    if (row.isNullAt(ordinal)) {
      return 0;
    }
    if (type instanceof StructType) {
      return hashRow(row.getStruct(ordinal), (StructType) type, includeTypeHash);
    }
    requireScalar(type, "hashing");
    return hashScalar(row, type, ordinal);
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

  private static boolean equalRows(
      Row left, Row right, StructType type, boolean validateSchema) {
    if (validateSchema && (!type.equals(left.getSchema()) || !type.equals(right.getSchema()))) {
      return false;
    }
    for (int ordinal = 0; ordinal < type.length(); ordinal++) {
      if (!equal(
          left, right, type.at(ordinal).getDataType(), ordinal, validateSchema)) {
        return false;
      }
    }
    return true;
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
          left.getStruct(ordinal),
          right.getStruct(ordinal),
          (StructType) type,
          validateSchema);
    }
    requireScalar(type, "equality");
    return equalScalar(left, right, type, ordinal);
  }

  private static boolean equalScalar(
      Row left, Row right, DataType type, int ordinal) {
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

  private static void requireScalar(DataType type, String operation) {
    if (!isScalar(type)) {
      throw new UnsupportedOperationException(
          "Unsupported type for Kernel value " + operation + ": " + type);
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
}
