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
import io.delta.kernel.defaults.internal.data.vector.AbstractDelegatingColumnVector;
import io.delta.kernel.expressions.ScalarExpression;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Evaluates the row-major {@code ARRAY(expr, ...)} constructor. */
final class ArrayExpressionEvaluator implements ExpressionKernel {
  static final ArrayExpressionEvaluator INSTANCE = new ArrayExpressionEvaluator();

  @Override
  public String name() {
    return "ARRAY";
  }

  @Override
  public DataType expectedChildType(DataType expectedType, int childIndex) {
    return expectedType instanceof ArrayType ? ((ArrayType) expectedType).getElementType() : null;
  }

  @Override
  public DataType resolve(
      ScalarExpression expression, List<DataType> childTypes, DataType expectedType) {
    if (childTypes.isEmpty()) {
      throw unsupportedExpressionException(
          expression, "Array expression requires at least one element");
    }
    if (expectedType != null && !(expectedType instanceof ArrayType)) {
      throw unsupportedExpressionException(
          expression, "Array expression requires an ArrayType result, but got " + expectedType);
    }
    DataType elementType = childTypes.get(0);
    for (DataType childType : childTypes) {
      if (!elementType.equals(childType)) {
        throw unsupportedExpressionException(
            expression, "Array expression inputs must have the same element type");
      }
    }
    if (expectedType == null) {
      return new ArrayType(elementType, true);
    }
    ArrayType arrayType = (ArrayType) expectedType;
    if (!arrayType.getElementType().equals(elementType)) {
      throw unsupportedExpressionException(
          expression,
          String.format(
              "Array element type %s does not match expected type %s",
              elementType, arrayType.getElementType()));
    }
    return arrayType;
  }

  @Override
  public ColumnVector eval(
      ScalarExpression expression,
      List<ColumnVector> elements,
      DataType outputType,
      int rowCount) {
    ArrayType arrayType = (ArrayType) outputType;
    DataType elementType = arrayType.getElementType();
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
      if (!arrayType.containsNull()) {
        validateNoNulls(element, index, rowCount);
      }
    }
    return new ArrayResultVector(arrayType, rowCount, elements);
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
  private static final class RowElementsVector extends AbstractDelegatingColumnVector {
    private final int rowId;
    private final List<ColumnVector> elements;

    private RowElementsVector(DataType dataType, int rowId, List<ColumnVector> elements) {
      super(elements.size(), dataType);
      this.rowId = rowId;
      this.elements = elements;
    }

    @Override
    protected ColumnVector delegateVector(int elementIndex) {
      return elements.get(elementIndex);
    }

    @Override
    protected int delegateRowId(int elementIndex) {
      return rowId;
    }
  }

  private ArrayExpressionEvaluator() {}
}
