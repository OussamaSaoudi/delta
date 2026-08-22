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
import io.delta.kernel.defaults.internal.data.DefaultValueRetainer;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import java.util.Arrays;

/** Provides a restricted view on an underlying column vector. */
public class DefaultViewVector extends AbstractDelegatingColumnVector
    implements RetainableColumnVector {

  private final ColumnVector underlyingVector;
  private final ColumnVector[] nullableParents;
  private final int offset;
  private final ColumnVector[] childViews;

  /**
   * @param underlyingVector the underlying column vector to read
   * @param start the row index of the underlyingVector where we want this vector to start
   * @param end the row index of the underlyingVector where we want this vector to end (exclusive)
   */
  public DefaultViewVector(ColumnVector underlyingVector, int start, int end) {
    this(underlyingVector, start, end, new ColumnVector[0]);
  }

  private DefaultViewVector(
      ColumnVector underlyingVector, int start, int end, ColumnVector[] nullableParents) {
    super(end - start, underlyingVector.getDataType());
    this.underlyingVector = underlyingVector;
    this.nullableParents = nullableParents;
    this.offset = start;
    DataType dataType = underlyingVector.getDataType();
    this.childViews =
        dataType instanceof StructType ? new ColumnVector[((StructType) dataType).length()] : null;
  }

  @Override
  public Object retainValue(int rowId) {
    checkValidRowId(rowId);
    int sourceRowId = offset + rowId;
    for (ColumnVector parent : nullableParents) {
      if (parent.isNullAt(sourceRowId)) {
        return null;
      }
    }
    return DefaultValueRetainer.retain(underlyingVector, getDataType(), sourceRowId);
  }

  @Override
  public boolean isNullAt(int rowId) {
    checkValidRowId(rowId);
    int sourceRowId = offset + rowId;
    for (ColumnVector parent : nullableParents) {
      if (parent.isNullAt(sourceRowId)) {
        return true;
      }
    }
    return underlyingVector.isNullAt(sourceRowId);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    if (childViews != null && ordinal >= 0 && ordinal < childViews.length) {
      ColumnVector child = childViews[ordinal];
      if (child == null) {
        child = createChild(ordinal);
        childViews[ordinal] = child;
      }
      return child;
    }
    // Delegate invalid ordinals and non-struct access to preserve the underlying error behavior.
    return createChild(ordinal);
  }

  private ColumnVector createChild(int ordinal) {
    ColumnVector[] parents = Arrays.copyOf(nullableParents, nullableParents.length + 1);
    parents[nullableParents.length] = underlyingVector;
    return new DefaultViewVector(
        underlyingVector.getChild(ordinal), offset, offset + getSize(), parents);
  }

  @Override
  protected ColumnVector delegateVector(int rowId) {
    return underlyingVector;
  }

  @Override
  protected int delegateRowId(int rowId) {
    return offset + rowId;
  }
}
