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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;

/** A nested column path resolved once against an input schema. */
public final class ColumnBinding {
  private final StructType inputSchema;
  private final DataType dataType;
  private final int[] ordinals;

  private ColumnBinding(StructType inputSchema, DataType dataType, int[] ordinals) {
    this.inputSchema = inputSchema;
    this.dataType = dataType;
    this.ordinals = ordinals;
  }

  public static ColumnBinding resolve(StructType schema, Column column) {
    requireNonNull(schema, "schema is null");
    String[] names = requireNonNull(column, "column is null").getNames();
    if (names.length == 0) {
      throw new IllegalArgumentException("Column path is empty");
    }
    int[] ordinals = new int[names.length];
    DataType current = schema;
    for (int level = 0; level < names.length; level++) {
      if (!(current instanceof StructType)) {
        throw unresolved(column, schema);
      }
      StructType struct = (StructType) current;
      int ordinal = struct.indexOf(requireNonNull(names[level], "column name is null"));
      if (ordinal < 0) {
        throw unresolved(column, schema);
      }
      ordinals[level] = ordinal;
      current = struct.at(ordinal).getDataType();
    }
    return new ColumnBinding(schema, current, ordinals);
  }

  public DataType getDataType() {
    return dataType;
  }

  /** Returns the borrowed vector for this column. */
  public ColumnVector getVector(ColumnarBatch batch) {
    requireNonNull(batch, "batch is null");
    if (!inputSchema.equals(batch.getSchema())) {
      throw new IllegalArgumentException("Batch schema does not match bound column schema");
    }
    return getVector(batch, ordinals);
  }

  public int[] getOrdinals() {
    return ordinals.clone();
  }

  public static ColumnVector getVector(ColumnarBatch batch, int[] ordinals) {
    requireNonNull(batch, "batch is null");
    requireNonNull(ordinals, "ordinals is null");
    if (ordinals.length == 0) {
      throw new IllegalArgumentException("Column path is empty");
    }
    ColumnVector vector = batch.getColumnVector(ordinals[0]);
    for (int level = 1; level < ordinals.length; level++) {
      vector = vector.getChild(ordinals[level]);
    }
    return vector;
  }

  private static IllegalArgumentException unresolved(Column column, StructType schema) {
    return new IllegalArgumentException("Cannot resolve " + column + " in schema " + schema);
  }
}
