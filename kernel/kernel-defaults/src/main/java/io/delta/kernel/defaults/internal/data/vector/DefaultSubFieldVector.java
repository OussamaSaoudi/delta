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
package io.delta.kernel.defaults.internal.data.vector;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultValueRetainer;
import io.delta.kernel.defaults.internal.data.RetainableRow;
import io.delta.kernel.internal.data.RowBackedColumnVector;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.function.IntFunction;

/**
 * {@link ColumnVector} wrapper on top of {@link Row} objects. This wrapper allows referencing any
 * nested level column vector from a set of rows.
 */
public class DefaultSubFieldVector extends RowBackedColumnVector implements RetainableColumnVector {

  /**
   * Create an instance of {@link DefaultSubFieldVector}
   *
   * @param size Number of elements in the vector
   * @param dataType Datatype of the vector
   * @param columnOrdinal ordinal of the column represented by this vector in the accessed rows
   * @param rowIdToRowAccessor returns the {@link Row} object for a row ID
   */
  public DefaultSubFieldVector(
      int size, DataType dataType, int columnOrdinal, IntFunction<Row> rowIdToRowAccessor) {
    super(size, dataType, columnOrdinal, rowIdToRowAccessor);
  }

  @Override
  public Object retainValue(int rowId) {
    Row row = rowAt(rowId);
    if (row instanceof RetainableRow) {
      return ((RetainableRow) row).retainValue(ordinal());
    }
    return DefaultValueRetainer.materialize(this, getDataType(), rowId);
  }

  @Override
  public ColumnVector getChild(int childOrdinal) {
    StructType structType = (StructType) getDataType();
    StructField childField = structType.at(childOrdinal);
    return new DefaultSubFieldVector(
        getSize(), childField.getDataType(), childOrdinal, this::structAt);
  }
}
