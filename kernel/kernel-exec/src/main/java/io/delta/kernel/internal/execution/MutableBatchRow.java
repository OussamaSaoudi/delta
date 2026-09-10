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
package io.delta.kernel.internal.execution;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.StructRow;
import io.delta.kernel.types.StructType;
import java.math.BigDecimal;

/** One reusable row view over a changing batch position. */
final class MutableBatchRow implements Row {
  private final StructType schema;
  private ColumnarBatch batch;
  private int rowId;

  MutableBatchRow(StructType schema) {
    this.schema = requireNonNull(schema, "schema is null");
  }

  void pointTo(ColumnarBatch batch, int rowId) {
    this.batch = requireNonNull(batch, "batch is null");
    if (rowId < 0 || rowId >= batch.getSize()) {
      throw new IndexOutOfBoundsException("Invalid row id: " + rowId);
    }
    this.rowId = rowId;
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return column(ordinal).isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return column(ordinal).getBoolean(rowId);
  }

  @Override
  public byte getByte(int ordinal) {
    return column(ordinal).getByte(rowId);
  }

  @Override
  public short getShort(int ordinal) {
    return column(ordinal).getShort(rowId);
  }

  @Override
  public int getInt(int ordinal) {
    return column(ordinal).getInt(rowId);
  }

  @Override
  public long getLong(int ordinal) {
    return column(ordinal).getLong(rowId);
  }

  @Override
  public float getFloat(int ordinal) {
    return column(ordinal).getFloat(rowId);
  }

  @Override
  public double getDouble(int ordinal) {
    return column(ordinal).getDouble(rowId);
  }

  @Override
  public String getString(int ordinal) {
    return column(ordinal).getString(rowId);
  }

  @Override
  public BigDecimal getDecimal(int ordinal) {
    return column(ordinal).getDecimal(rowId);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return column(ordinal).getBinary(rowId);
  }

  @Override
  public Row getStruct(int ordinal) {
    return StructRow.fromStructVector(column(ordinal), rowId);
  }

  @Override
  public ArrayValue getArray(int ordinal) {
    return column(ordinal).getArray(rowId);
  }

  @Override
  public MapValue getMap(int ordinal) {
    return column(ordinal).getMap(rowId);
  }

  private ColumnVector column(int ordinal) {
    if (batch == null) {
      throw new IllegalStateException("Row has no batch position");
    }
    if (ordinal < 0 || ordinal >= schema.length()) {
      throw new IndexOutOfBoundsException("Invalid ordinal: " + ordinal);
    }
    return batch.getColumnVector(ordinal);
  }
}
