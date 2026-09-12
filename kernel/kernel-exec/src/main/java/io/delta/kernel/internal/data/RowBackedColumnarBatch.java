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
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

/** A zero-copy execution batch over independently addressable Kernel rows. */
public final class RowBackedColumnarBatch implements ColumnarBatch {
  private final StructType schema;
  private final List<? extends Row> rows;
  private final int size;
  private final ColumnVector[] columns;
  private final Lifetime lifetime;

  public RowBackedColumnarBatch(StructType schema, List<? extends Row> rows, Lifetime lifetime) {
    this.schema = requireNonNull(schema, "schema is null");
    this.rows = requireNonNull(rows, "rows is null");
    this.size = this.rows.size();
    this.columns = new ColumnVector[schema.length()];
    this.lifetime = requireNonNull(lifetime, "lifetime is null");
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  @Override
  public int getSize() {
    return size;
  }

  @Override
  public Lifetime getLifetime() {
    return lifetime;
  }

  @Override
  public ColumnVector getColumnVector(int ordinal) {
    checkArgument(ordinal >= 0 && ordinal < columns.length, "Invalid ordinal: %s", ordinal);
    ColumnVector column = columns[ordinal];
    if (column == null) {
      column = createColumn(schema.at(ordinal), ordinal, rows::get);
      columns[ordinal] = column;
    }
    return column;
  }

  /** Returns one backing row without constructing a positional view. */
  public Row getRow(int rowId) {
    checkArgument(rowId >= 0 && rowId < size, "Invalid rowId: %s", rowId);
    return rows.get(rowId);
  }

  public ColumnarBatch filter(boolean[] keep) {
    checkArgument(keep != null && keep.length == size, "Filter length must match batch size");
    boolean allKept = true;
    for (boolean selected : keep) {
      allKept &= selected;
    }
    if (allKept) {
      return this;
    }
    List<Row> selected = new ArrayList<>();
    for (int rowId = 0; rowId < size; rowId++) {
      if (keep[rowId]) {
        selected.add(rows.get(rowId));
      }
    }
    return new RowBackedColumnarBatch(schema, selected, lifetime);
  }

  private ColumnVector createColumn(
      StructField field, int ordinal, IntFunction<? extends Row> rowAccess) {
    return new RowColumnVector(size, field.getDataType(), ordinal, rowAccess);
  }

  /** One column view over the backing rows; nested child views are created on demand. */
  private static final class RowColumnVector implements ColumnVector {
    private final int size;
    private final DataType dataType;
    private final int ordinal;
    private final IntFunction<? extends Row> rows;
    private final ColumnVector[] children;

    private RowColumnVector(
        int size, DataType dataType, int ordinal, IntFunction<? extends Row> rows) {
      this.size = size;
      this.dataType = requireNonNull(dataType, "dataType is null");
      this.ordinal = ordinal;
      this.rows = requireNonNull(rows, "rows is null");
      this.children =
          dataType instanceof StructType
              ? new ColumnVector[((StructType) dataType).length()]
              : null;
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
      ColumnVector child = children[childOrdinal];
      if (child == null) {
        StructField field = ((StructType) dataType).at(childOrdinal);
        child = new RowColumnVector(size, field.getDataType(), childOrdinal, this::structAt);
        children[childOrdinal] = child;
      }
      return child;
    }

    private Row rowAt(int rowId) {
      checkArgument(rowId >= 0 && rowId < size, "Invalid rowId: %s", rowId);
      return rows.apply(rowId);
    }

    private Row structAt(int rowId) {
      Row row = rowAt(rowId);
      return row == null || row.isNullAt(ordinal) ? null : row.getStruct(ordinal);
    }
  }
}
