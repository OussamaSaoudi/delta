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
package io.delta.kernel.internal.data;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.util.RowKernels;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.math.BigDecimal;
import java.util.function.IntFunction;

/** A zero-copy vector view over one ordinal of independently addressable rows. */
public class RowBackedColumnVector implements ColumnVector {
  private final int size;
  private final DataType dataType;
  private final int ordinal;
  private final IntFunction<Row> rows;
  private final ColumnVector[] children;

  public RowBackedColumnVector(int size, DataType dataType, int ordinal, IntFunction<Row> rows) {
    checkArgument(size >= 0, "Invalid size: %s", size);
    checkArgument(ordinal >= 0, "Invalid ordinal: %s", ordinal);
    this.size = size;
    this.dataType = requireNonNull(dataType, "dataType is null");
    this.ordinal = ordinal;
    this.rows = requireNonNull(rows, "rows is null");
    this.children =
        dataType instanceof StructType ? new ColumnVector[((StructType) dataType).length()] : null;
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  @Override
  public int getSize() {
    return size;
  }

  @Override
  public void close() {}

  @Override
  public boolean isNullAt(int rowId) {
    Row row = rowAt(rowId);
    return row == null || row.isNullAt(ordinal);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return rowAt(rowId).getBoolean(ordinal);
  }

  @Override
  public byte getByte(int rowId) {
    return rowAt(rowId).getByte(ordinal);
  }

  @Override
  public short getShort(int rowId) {
    return rowAt(rowId).getShort(ordinal);
  }

  @Override
  public int getInt(int rowId) {
    return rowAt(rowId).getInt(ordinal);
  }

  @Override
  public long getLong(int rowId) {
    return rowAt(rowId).getLong(ordinal);
  }

  @Override
  public float getFloat(int rowId) {
    return rowAt(rowId).getFloat(ordinal);
  }

  @Override
  public double getDouble(int rowId) {
    return rowAt(rowId).getDouble(ordinal);
  }

  @Override
  public String getString(int rowId) {
    return rowAt(rowId).getString(ordinal);
  }

  @Override
  public byte[] getBinary(int rowId) {
    return rowAt(rowId).getBinary(ordinal);
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    return rowAt(rowId).getDecimal(ordinal);
  }

  @Override
  public ArrayValue getArray(int rowId) {
    return rowAt(rowId).getArray(ordinal);
  }

  @Override
  public MapValue getMap(int rowId) {
    return rowAt(rowId).getMap(ordinal);
  }

  @Override
  public ColumnVector getChild(int childOrdinal) {
    checkArgument(children != null, "Child vectors are unavailable for %s", dataType);
    checkArgument(
        childOrdinal >= 0 && childOrdinal < children.length,
        "Invalid child ordinal: %s",
        childOrdinal);
    if (children[childOrdinal] != null) {
      return children[childOrdinal];
    }
    StructType struct = (StructType) dataType;
    StructField child = struct.at(childOrdinal);
    ColumnVector result =
        new RowBackedColumnVector(size, child.getDataType(), childOrdinal, this::structAt);
    children[childOrdinal] = result;
    return result;
  }

  /** Returns the backing struct row without changing its representation. */
  public Row getStruct(int rowId) {
    checkArgument(dataType instanceof StructType, "Struct rows are unavailable for %s", dataType);
    return structAt(rowId);
  }

  /** Returns an owned value, using the backing row's native representation when available. */
  public Object retainValue(int rowId) {
    Row row = rowAt(rowId);
    if (row instanceof RetainableRow) {
      return ((RetainableRow) row).retainValue(ordinal);
    }
    return RowKernels.materialize(row, dataType, ordinal);
  }

  protected final int ordinal() {
    return ordinal;
  }

  protected final Row rowAt(int rowId) {
    checkArgument(rowId >= 0 && rowId < size, "Invalid rowId: %s", rowId);
    return rows.apply(rowId);
  }

  protected final Row structAt(int rowId) {
    Row row = rowAt(rowId);
    return row == null || row.isNullAt(ordinal) ? null : row.getStruct(ordinal);
  }
}
