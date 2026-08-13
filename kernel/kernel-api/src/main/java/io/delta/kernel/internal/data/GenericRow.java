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

package io.delta.kernel.internal.data;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.data.VariantValue;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Exposes ordinal values as a {@link Row}. */
public class GenericRow implements Row {
  private final StructType schema;
  private final Map<Integer, ?> ordinalToValue;
  private final List<?> ordinalValues;

  /**
   * @param schema the schema of the row
   * @param ordinalToValue a mapping of column ordinal to objects; for each column the object must
   *     be of the return type corresponding to the data type's getter method in the Row interface
   */
  public GenericRow(StructType schema, Map<Integer, ?> ordinalToValue) {
    this.schema = requireNonNull(schema, "schema is null");
    this.ordinalToValue = requireNonNull(ordinalToValue, "ordinalToValue is null");
    this.ordinalValues = null;
    validateVoidValues();
  }

  /**
   * Creates a dense row whose values are in schema ordinal order.
   *
   * <p>Each value must be of the return type corresponding to the data type's getter method in the
   * {@link Row} interface. The values are copied so subsequent changes to the input list do not
   * affect the row.
   *
   * @param schema the schema of the row
   * @param ordinalValues one value for every field in {@code schema}, in ordinal order
   */
  public static GenericRow fromValues(StructType schema, List<?> ordinalValues) {
    return new GenericRow(schema, ordinalValues);
  }

  private GenericRow(StructType schema, List<?> ordinalValues) {
    this.schema = requireNonNull(schema, "schema is null");
    requireNonNull(ordinalValues, "ordinalValues is null");
    if (ordinalValues.size() != schema.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Expected %s values for row schema, got %s", schema.length(), ordinalValues.size()));
    }
    this.ordinalToValue = null;
    this.ordinalValues = Collections.unmodifiableList(new ArrayList<Object>(ordinalValues));
    validateVoidValues();
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    dataType(ordinal);
    return getValue(ordinal) == null;
  }

  @Override
  public boolean getBoolean(int ordinal) {
    throwIfUnsafeAccess(ordinal, "boolean", BooleanType.class);
    return (boolean) getValue(ordinal);
  }

  @Override
  public byte getByte(int ordinal) {
    throwIfUnsafeAccess(ordinal, "byte", ByteType.class);
    return (byte) getValue(ordinal);
  }

  @Override
  public short getShort(int ordinal) {
    throwIfUnsafeAccess(ordinal, "short", ShortType.class);
    return (short) getValue(ordinal);
  }

  @Override
  public int getInt(int ordinal) {
    throwIfUnsafeAccess(ordinal, "integer", IntegerType.class, DateType.class);
    return (int) getValue(ordinal);
  }

  @Override
  public long getLong(int ordinal) {
    throwIfUnsafeAccess(
        ordinal, "long", LongType.class, TimestampType.class, TimestampNTZType.class);
    return (long) getValue(ordinal);
  }

  @Override
  public int getIntervalYearMonth(int ordinal) {
    throwIfUnsafeAccess(ordinal, "interval year to month", IntervalYearMonthType.class);
    return (int) getValue(ordinal);
  }

  @Override
  public long getIntervalDayTime(int ordinal) {
    throwIfUnsafeAccess(ordinal, "interval day to second", IntervalDayTimeType.class);
    return (long) getValue(ordinal);
  }

  @Override
  public float getFloat(int ordinal) {
    throwIfUnsafeAccess(ordinal, "float", FloatType.class);
    return (float) getValue(ordinal);
  }

  @Override
  public double getDouble(int ordinal) {
    throwIfUnsafeAccess(ordinal, "double", DoubleType.class);
    return (double) getValue(ordinal);
  }

  @Override
  public String getString(int ordinal) {
    throwIfUnsafeAccess(
        ordinal, "string", StringType.class, GeometryType.class, GeographyType.class);
    return (String) getValue(ordinal);
  }

  @Override
  public BigDecimal getDecimal(int ordinal) {
    throwIfUnsafeAccess(ordinal, "decimal", DecimalType.class);
    return (BigDecimal) getValue(ordinal);
  }

  @Override
  public VariantValue getVariant(int ordinal) {
    throwIfUnsafeAccess(ordinal, "variant", VariantType.class);
    return (VariantValue) getValue(ordinal);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    throwIfUnsafeAccess(ordinal, "binary", BinaryType.class);
    return (byte[]) getValue(ordinal);
  }

  @Override
  public Row getStruct(int ordinal) {
    throwIfUnsafeAccess(ordinal, "struct", StructType.class);
    return (Row) getValue(ordinal);
  }

  @Override
  public ArrayValue getArray(int ordinal) {
    // TODO: not sufficient check, also need to check the element type
    throwIfUnsafeAccess(ordinal, "array", ArrayType.class);
    return (ArrayValue) getValue(ordinal);
  }

  @Override
  public MapValue getMap(int ordinal) {
    // TODO: not sufficient check, also need to check the element types
    throwIfUnsafeAccess(ordinal, "map", MapType.class);
    return (MapValue) getValue(ordinal);
  }

  private Object getValue(int ordinal) {
    return ordinalValues != null ? ordinalValues.get(ordinal) : ordinalToValue.get(ordinal);
  }

  private void validateVoidValues() {
    for (int ordinal = 0; ordinal < schema.length(); ordinal++) {
      if (schema.at(ordinal).getDataType() instanceof VoidType && getValue(ordinal) != null) {
        throw new IllegalArgumentException(
            String.format("Void value at ordinal %s must be null", ordinal));
      }
    }
  }

  private void throwIfUnsafeAccess(int ordinal, String accessType, Class<?>... expectedDataTypes) {
    DataType actualDataType = dataType(ordinal);
    for (Class<?> expectedDataType : expectedDataTypes) {
      if (expectedDataType.isAssignableFrom(actualDataType.getClass())) {
        return;
      }
    }
    throw new UnsupportedOperationException(
        String.format(
            "Trying to access a `%s` value from vector of type `%s`", accessType, actualDataType));
  }

  private DataType dataType(int ordinal) {
    if (ordinal < 0 || schema.length() <= ordinal) {
      throw new IllegalArgumentException("invalid ordinal: " + ordinal);
    }

    return schema.at(ordinal).getDataType();
  }
}
