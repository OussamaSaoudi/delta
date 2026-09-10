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
package io.delta.spark.internal.v2.kernel;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructType;
import java.math.BigDecimal;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;

/** A zero-copy Kernel columnar view over independently addressable Spark rows. */
public final class SparkRowArrayBatch implements ColumnarBatch {
  private final StructType kernelSchema;
  private final org.apache.spark.sql.types.StructType sparkSchema;
  private final InternalRow[] rows;
  private final int size;
  private final ColumnVector[] columns;

  public SparkRowArrayBatch(
      StructType kernelSchema,
      org.apache.spark.sql.types.StructType sparkSchema,
      InternalRow[] rows,
      int size) {
    this.kernelSchema = requireNonNull(kernelSchema, "kernelSchema is null");
    this.sparkSchema = requireNonNull(sparkSchema, "sparkSchema is null");
    this.rows = requireNonNull(rows, "rows is null");
    if (size < 0 || size > rows.length) {
      throw new IllegalArgumentException(
          "size must be between zero and the backing array length: " + size);
    }
    if (kernelSchema.length() != sparkSchema.fields().length) {
      throw new IllegalArgumentException("Kernel and Spark schemas have different field counts");
    }
    for (int rowId = 0; rowId < size; rowId++) {
      requireNonNull(rows[rowId], "row " + rowId + " is null");
    }
    this.size = size;
    this.columns = new ColumnVector[kernelSchema.length()];
  }

  @Override
  public StructType getSchema() {
    return kernelSchema;
  }

  @Override
  public int getSize() {
    return size;
  }

  @Override
  public ColumnVector getColumnVector(int ordinal) {
    checkOrdinal(ordinal, columns.length);
    ColumnVector column = columns[ordinal];
    if (column == null) {
      DataType kernelType = kernelSchema.at(ordinal).getDataType();
      org.apache.spark.sql.types.DataType sparkType = sparkSchema.fields()[ordinal].dataType();
      column =
          new RowBackedColumnVector(
              size, kernelType, sparkType, rowId -> rows[rowId], ignored -> ordinal);
      columns[ordinal] = column;
    }
    return column;
  }

  /** Fast-path seam for Spark expression evaluation. */
  InternalRow backingRow(int rowId) {
    checkOrdinal(rowId, size);
    return rows[rowId];
  }

  private static void checkOrdinal(int ordinal, int upperBound) {
    if (ordinal < 0 || ordinal >= upperBound) {
      throw new IndexOutOfBoundsException("Invalid ordinal: " + ordinal);
    }
  }

  /** A vector whose values live at positions in Spark's existing getter containers. */
  private static final class RowBackedColumnVector implements ColumnVector {
    private final int size;
    private final DataType kernelType;
    private final org.apache.spark.sql.types.DataType sparkType;
    private final IntFunction<SpecializedGetters> containers;
    private final IntUnaryOperator ordinals;
    private final IntFunction<SpecializedGetters> structRows;
    private final ColumnVector[] children;

    private RowBackedColumnVector(
        int size,
        DataType kernelType,
        org.apache.spark.sql.types.DataType sparkType,
        IntFunction<SpecializedGetters> containers,
        IntUnaryOperator ordinals) {
      this.size = size;
      this.kernelType = requireNonNull(kernelType, "kernelType is null");
      this.sparkType = requireNonNull(sparkType, "sparkType is null");
      this.containers = requireNonNull(containers, "containers is null");
      this.ordinals = requireNonNull(ordinals, "ordinals is null");
      if (kernelType instanceof StructType
          && sparkType instanceof org.apache.spark.sql.types.StructType) {
        StructType kernelStruct = (StructType) kernelType;
        org.apache.spark.sql.types.StructType sparkStruct =
            (org.apache.spark.sql.types.StructType) sparkType;
        if (kernelStruct.length() != sparkStruct.fields().length) {
          throw new IllegalArgumentException(
              "Nested Kernel and Spark schemas have different field counts");
        }
        this.children = new ColumnVector[kernelStruct.length()];
        this.structRows =
            rowId -> {
              SpecializedGetters container = container(rowId);
              int ordinal = ordinal(rowId);
              return container == null || container.isNullAt(ordinal)
                  ? null
                  : container.getStruct(ordinal, kernelStruct.length());
            };
      } else {
        this.children = null;
        this.structRows = null;
      }
    }

    @Override
    public DataType getDataType() {
      return kernelType;
    }

    @Override
    public int getSize() {
      return size;
    }

    @Override
    public void close() {}

    @Override
    public boolean isNullAt(int rowId) {
      SpecializedGetters container = container(rowId);
      return container == null || container.isNullAt(ordinal(rowId));
    }

    @Override
    public boolean getBoolean(int rowId) {
      return container(rowId).getBoolean(ordinal(rowId));
    }

    @Override
    public byte getByte(int rowId) {
      return container(rowId).getByte(ordinal(rowId));
    }

    @Override
    public short getShort(int rowId) {
      return container(rowId).getShort(ordinal(rowId));
    }

    @Override
    public int getInt(int rowId) {
      return container(rowId).getInt(ordinal(rowId));
    }

    @Override
    public long getLong(int rowId) {
      return container(rowId).getLong(ordinal(rowId));
    }

    @Override
    public float getFloat(int rowId) {
      return container(rowId).getFloat(ordinal(rowId));
    }

    @Override
    public double getDouble(int rowId) {
      return container(rowId).getDouble(ordinal(rowId));
    }

    @Override
    public String getString(int rowId) {
      return container(rowId).getUTF8String(ordinal(rowId)).toString();
    }

    @Override
    public byte[] getBinary(int rowId) {
      return container(rowId).getBinary(ordinal(rowId));
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
      DecimalType kernelDecimal = (DecimalType) kernelType;
      return container(rowId)
          .getDecimal(ordinal(rowId), kernelDecimal.getPrecision(), kernelDecimal.getScale())
          .toJavaBigDecimal();
    }

    @Override
    public ColumnVector getChild(int childOrdinal) {
      if (children == null) {
        throw new UnsupportedOperationException(
            "Child vectors are not available for vector of type " + kernelType);
      }
      checkOrdinal(childOrdinal, children.length);
      ColumnVector child = children[childOrdinal];
      if (child == null) {
        StructType kernelStruct = (StructType) kernelType;
        org.apache.spark.sql.types.StructType sparkStruct =
            (org.apache.spark.sql.types.StructType) sparkType;
        child =
            new RowBackedColumnVector(
                size,
                kernelStruct.at(childOrdinal).getDataType(),
                sparkStruct.fields()[childOrdinal].dataType(),
                structRows,
                ignored -> childOrdinal);
        children[childOrdinal] = child;
      }
      return child;
    }

    @Override
    public ArrayValue getArray(int rowId) {
      ArrayData array = container(rowId).getArray(ordinal(rowId));
      return new SparkArrayValue(
          array, (ArrayType) kernelType, (org.apache.spark.sql.types.ArrayType) sparkType);
    }

    @Override
    public MapValue getMap(int rowId) {
      MapData map = container(rowId).getMap(ordinal(rowId));
      return new SparkMapValue(
          map, (MapType) kernelType, (org.apache.spark.sql.types.MapType) sparkType);
    }

    private SpecializedGetters container(int rowId) {
      checkOrdinal(rowId, size);
      return containers.apply(rowId);
    }

    private int ordinal(int rowId) {
      return ordinals.applyAsInt(rowId);
    }
  }

  /** A lazy vector view over one existing Spark array. */
  private static final class SparkArrayValue implements ArrayValue {
    private final ArrayData array;
    private final ArrayType kernelType;
    private final org.apache.spark.sql.types.ArrayType sparkType;
    private ColumnVector elements;

    private SparkArrayValue(
        ArrayData array, ArrayType kernelType, org.apache.spark.sql.types.ArrayType sparkType) {
      this.array = requireNonNull(array, "array is null");
      this.kernelType = requireNonNull(kernelType, "kernelType is null");
      this.sparkType = requireNonNull(sparkType, "sparkType is null");
    }

    @Override
    public int getSize() {
      return array.numElements();
    }

    @Override
    public ColumnVector getElements() {
      if (elements == null) {
        elements = arrayVector(array, kernelType.getElementType(), sparkType.elementType());
      }
      return elements;
    }
  }

  /** Lazy key and value vector views over one existing Spark map. */
  private static final class SparkMapValue implements MapValue {
    private final MapData map;
    private final MapType kernelType;
    private final org.apache.spark.sql.types.MapType sparkType;
    private ColumnVector keys;
    private ColumnVector values;

    private SparkMapValue(
        MapData map, MapType kernelType, org.apache.spark.sql.types.MapType sparkType) {
      this.map = requireNonNull(map, "map is null");
      this.kernelType = requireNonNull(kernelType, "kernelType is null");
      this.sparkType = requireNonNull(sparkType, "sparkType is null");
    }

    @Override
    public int getSize() {
      return map.numElements();
    }

    @Override
    public ColumnVector getKeys() {
      if (keys == null) {
        keys = arrayVector(map.keyArray(), kernelType.getKeyType(), sparkType.keyType());
      }
      return keys;
    }

    @Override
    public ColumnVector getValues() {
      if (values == null) {
        values = arrayVector(map.valueArray(), kernelType.getValueType(), sparkType.valueType());
      }
      return values;
    }
  }

  private static ColumnVector arrayVector(
      ArrayData array, DataType kernelType, org.apache.spark.sql.types.DataType sparkType) {
    return new RowBackedColumnVector(
        array.numElements(), kernelType, sparkType, ignored -> array, index -> index);
  }
}
