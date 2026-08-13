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
package io.delta.kernel.defaults.internal.expressions;

import static io.delta.kernel.defaults.internal.DefaultEngineErrors.unsupportedExpressionException;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.VariantValue;
import io.delta.kernel.expressions.ScalarExpression;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Evaluates the row-major {@code ARRAY(expr, ...)} constructor. */
final class ArrayExpressionEvaluator {
  static ColumnVector eval(
      ScalarExpression expression,
      List<ColumnVector> elements,
      ArrayType outputType,
      int rowCount) {
    if (elements.isEmpty()) {
      throw unsupportedExpressionException(
          expression, "Array expression requires at least one element");
    }
    DataType elementType = outputType.getElementType();
    for (int index = 0; index < elements.size(); index++) {
      ColumnVector element = elements.get(index);
      checkArgument(
          element.getSize() == rowCount,
          "Array expression input %s has size %s, expected %s",
          index,
          element.getSize(),
          rowCount);
      checkArgument(
          elementType.equals(element.getDataType()),
          "Array expression input %s has type %s, expected %s",
          index,
          element.getDataType(),
          elementType);
      if (!outputType.containsNull()) {
        validateNoNulls(element, index, rowCount);
      }
    }
    return new ArrayResultVector(outputType, rowCount, elements);
  }

  private static void validateNoNulls(ColumnVector element, int inputIndex, int rowCount) {
    for (int rowId = 0; rowId < rowCount; rowId++) {
      checkArgument(
          !element.isNullAt(rowId),
          "Array expression declares non-nullable elements but input %s is null at row %s",
          inputIndex,
          rowId);
    }
  }

  private static final class ArrayResultVector implements ColumnVector {
    private final ArrayType dataType;
    private final int size;
    private final List<ColumnVector> elements;
    private boolean closed;

    private ArrayResultVector(ArrayType dataType, int size, List<ColumnVector> elements) {
      this.dataType = dataType;
      this.size = size;
      this.elements = Collections.unmodifiableList(new ArrayList<>(elements));
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
    public void close() {
      if (!closed) {
        closed = true;
        Utils.closeCloseables(elements.toArray(new ColumnVector[0]));
      }
    }

    @Override
    public boolean isNullAt(int rowId) {
      checkRowId(rowId);
      return false;
    }

    @Override
    public ArrayValue getArray(int rowId) {
      checkRowId(rowId);
      ColumnVector rowElements = new RowElementsVector(dataType.getElementType(), rowId, elements);
      return new ArrayValue() {
        @Override
        public int getSize() {
          return rowElements.getSize();
        }

        @Override
        public ColumnVector getElements() {
          return rowElements;
        }
      };
    }

    private void checkRowId(int rowId) {
      checkArgument(rowId >= 0 && rowId < size, "Invalid rowId %s for size %s", rowId, size);
    }
  }

  /** A non-owning view of one input row across every evaluated ARRAY argument. */
  private static final class RowElementsVector implements ColumnVector {
    private final DataType dataType;
    private final int rowId;
    private final List<ColumnVector> elements;

    private RowElementsVector(DataType dataType, int rowId, List<ColumnVector> elements) {
      this.dataType = dataType;
      this.rowId = rowId;
      this.elements = elements;
    }

    @Override
    public DataType getDataType() {
      return dataType;
    }

    @Override
    public int getSize() {
      return elements.size();
    }

    @Override
    public void close() {}

    @Override
    public boolean isNullAt(int elementIndex) {
      return element(elementIndex).isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int elementIndex) {
      return element(elementIndex).getBoolean(rowId);
    }

    @Override
    public byte getByte(int elementIndex) {
      return element(elementIndex).getByte(rowId);
    }

    @Override
    public short getShort(int elementIndex) {
      return element(elementIndex).getShort(rowId);
    }

    @Override
    public int getInt(int elementIndex) {
      return element(elementIndex).getInt(rowId);
    }

    @Override
    public long getLong(int elementIndex) {
      return element(elementIndex).getLong(rowId);
    }

    @Override
    public int getIntervalYearMonth(int elementIndex) {
      return element(elementIndex).getIntervalYearMonth(rowId);
    }

    @Override
    public long getIntervalDayTime(int elementIndex) {
      return element(elementIndex).getIntervalDayTime(rowId);
    }

    @Override
    public float getFloat(int elementIndex) {
      return element(elementIndex).getFloat(rowId);
    }

    @Override
    public double getDouble(int elementIndex) {
      return element(elementIndex).getDouble(rowId);
    }

    @Override
    public byte[] getBinary(int elementIndex) {
      return element(elementIndex).getBinary(rowId);
    }

    @Override
    public String getString(int elementIndex) {
      return element(elementIndex).getString(rowId);
    }

    @Override
    public BigDecimal getDecimal(int elementIndex) {
      return element(elementIndex).getDecimal(rowId);
    }

    @Override
    public VariantValue getVariant(int elementIndex) {
      return element(elementIndex).getVariant(rowId);
    }

    @Override
    public MapValue getMap(int elementIndex) {
      return element(elementIndex).getMap(rowId);
    }

    @Override
    public ArrayValue getArray(int elementIndex) {
      return element(elementIndex).getArray(rowId);
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      List<ColumnVector> children = new ArrayList<>(elements.size());
      for (ColumnVector element : elements) {
        children.add(element.getChild(ordinal));
      }
      return new RowElementsVector(children.get(0).getDataType(), rowId, children);
    }

    private ColumnVector element(int elementIndex) {
      checkArgument(
          elementIndex >= 0 && elementIndex < elements.size(),
          "Invalid element index %s for size %s",
          elementIndex,
          elements.size());
      return elements.get(elementIndex);
    }
  }

  private ArrayExpressionEvaluator() {}
}
