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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.IntFunction;

/** A zero-copy columnar view over independently addressable kernel rows. */
public final class RowBackedColumnarBatch implements ColumnarBatch {
  private final StructType schema;
  private final List<Row> rows;
  private final int size;
  private final ColumnVector[] columns;

  public RowBackedColumnarBatch(StructType schema, List<? extends Row> rows) {
    this.schema = requireNonNull(schema, "schema is null");
    requireNonNull(rows, "rows is null");
    this.rows = Collections.unmodifiableList(new ArrayList<>(rows));
    this.size = this.rows.size();
    this.columns = createColumns(schema, rowId -> this.rows.get(rowId));
  }

  private RowBackedColumnarBatch(StructType schema, int size, ColumnVector[] columns) {
    this.schema = requireNonNull(schema, "schema is null");
    checkArgument(size >= 0, "Invalid size: %s", size);
    this.rows = null;
    this.size = size;
    this.columns = requireNonNull(columns, "columns is null");
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
  public ColumnVector getColumnVector(int ordinal) {
    checkArgument(ordinal >= 0 && ordinal < columns.length, "Invalid ordinal: %s", ordinal);
    return columns[ordinal];
  }

  @Override
  public ColumnarBatch withNewColumn(
      int ordinal, StructField columnSchema, ColumnVector columnVector) {
    checkArgument(ordinal >= 0 && ordinal <= columns.length, "Invalid ordinal: %s", ordinal);
    requireNonNull(columnSchema, "columnSchema is null");
    requireNonNull(columnVector, "columnVector is null");
    checkArgument(
        columnVector.getSize() == size,
        "Column size %s does not match batch size %s",
        columnVector.getSize(),
        size);
    checkArgument(
        columnSchema.getDataType().equals(columnVector.getDataType()),
        "Column type %s does not match field type %s",
        columnVector.getDataType(),
        columnSchema.getDataType());

    List<StructField> fields = new ArrayList<>(schema.fields());
    fields.add(ordinal, columnSchema);
    ColumnVector[] result = new ColumnVector[columns.length + 1];
    System.arraycopy(columns, 0, result, 0, ordinal);
    result[ordinal] = columnVector;
    System.arraycopy(columns, ordinal, result, ordinal + 1, columns.length - ordinal);
    return new RowBackedColumnarBatch(new StructType(fields), size, result);
  }

  @Override
  public io.delta.kernel.utils.CloseableIterator<Row> getRows() {
    return rows == null
        ? ColumnarBatch.super.getRows()
        : io.delta.kernel.internal.util.Utils.toCloseableIterator(rows.iterator());
  }

  private ColumnVector[] createColumns(StructType rowSchema, IntFunction<Row> rows) {
    ColumnVector[] result = new ColumnVector[rowSchema.length()];
    for (int ordinal = 0; ordinal < result.length; ordinal++) {
      StructField field = rowSchema.at(ordinal);
      result[ordinal] = new RowBackedColumnVector(size, field.getDataType(), ordinal, rows);
    }
    return result;
  }
}
