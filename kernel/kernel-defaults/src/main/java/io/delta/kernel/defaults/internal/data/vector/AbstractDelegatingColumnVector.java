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
package io.delta.kernel.defaults.internal.data.vector;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import java.math.BigDecimal;

/** A non-owning vector that maps each output row to a row in another vector. */
public abstract class AbstractDelegatingColumnVector implements ColumnVector {
  private final int size;
  private final DataType dataType;

  protected AbstractDelegatingColumnVector(int size, DataType dataType) {
    checkArgument(size >= 0, "Invalid vector size: %s", size);
    this.size = size;
    this.dataType = dataType;
  }

  protected abstract ColumnVector delegateVector(int rowId);

  protected abstract int delegateRowId(int rowId);

  @Override
  public final DataType getDataType() {
    return dataType;
  }

  @Override
  public final int getSize() {
    return size;
  }

  @Override
  public void close() {}

  @Override
  public boolean isNullAt(int rowId) {
    checkValidRowId(rowId);
    return delegateVector(rowId).isNullAt(delegateRowId(rowId));
  }

  @Override
  public boolean getBoolean(int rowId) {
    return delegate(rowId).getBoolean(delegateRowId(rowId));
  }

  @Override
  public byte getByte(int rowId) {
    return delegate(rowId).getByte(delegateRowId(rowId));
  }

  @Override
  public short getShort(int rowId) {
    return delegate(rowId).getShort(delegateRowId(rowId));
  }

  @Override
  public int getInt(int rowId) {
    return delegate(rowId).getInt(delegateRowId(rowId));
  }

  @Override
  public long getLong(int rowId) {
    return delegate(rowId).getLong(delegateRowId(rowId));
  }

  @Override
  public float getFloat(int rowId) {
    return delegate(rowId).getFloat(delegateRowId(rowId));
  }

  @Override
  public double getDouble(int rowId) {
    return delegate(rowId).getDouble(delegateRowId(rowId));
  }

  @Override
  public byte[] getBinary(int rowId) {
    return delegate(rowId).getBinary(delegateRowId(rowId));
  }

  @Override
  public String getString(int rowId) {
    return delegate(rowId).getString(delegateRowId(rowId));
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    return delegate(rowId).getDecimal(delegateRowId(rowId));
  }

  @Override
  public MapValue getMap(int rowId) {
    return delegate(rowId).getMap(delegateRowId(rowId));
  }

  @Override
  public ArrayValue getArray(int rowId) {
    return delegate(rowId).getArray(delegateRowId(rowId));
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    checkArgument(dataType instanceof StructType, "Vector is not a struct: %s", dataType);
    DataType childType = ((StructType) dataType).at(ordinal).getDataType();
    return new AbstractDelegatingColumnVector(size, childType) {
      @Override
      protected ColumnVector delegateVector(int rowId) {
        return AbstractDelegatingColumnVector.this.delegate(rowId).getChild(ordinal);
      }

      @Override
      protected int delegateRowId(int rowId) {
        return AbstractDelegatingColumnVector.this.delegateRowId(rowId);
      }
    };
  }

  protected void checkValidRowId(int rowId) {
    if (rowId < 0 || rowId >= size) {
      throw new IllegalArgumentException("Invalid rowId " + rowId + " for size " + size);
    }
  }

  private ColumnVector delegate(int rowId) {
    checkValidRowId(rowId);
    return delegateVector(rowId);
  }
}
