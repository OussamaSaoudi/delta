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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.defaults.internal.data.vector.AbstractDelegatingColumnVector;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.expressions.ScalarExpression;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.List;

/** Kernel-native evaluation for {@code TO_JSON}. */
final class ToJsonExpressionEvaluator implements ExpressionKernel {
  static final ToJsonExpressionEvaluator INSTANCE = new ToJsonExpressionEvaluator();

  private ToJsonExpressionEvaluator() {}

  @Override
  public String name() {
    return "TO_JSON";
  }

  @Override
  public DataType resolve(
      ScalarExpression expression, List<DataType> childTypes, DataType expectedType) {
    if (childTypes.size() != 1 || !(childTypes.get(0) instanceof StructType)) {
      throw unsupportedExpressionException(expression, "TO_JSON requires exactly one struct input");
    }
    if (expectedType != null && !StringType.STRING.equals(expectedType)) {
      throw unsupportedExpressionException(
          expression, "TO_JSON requires a string result, but got " + expectedType);
    }
    return StringType.STRING;
  }

  @Override
  public ColumnVector eval(
      ScalarExpression expression, List<ColumnVector> children, DataType outputType, int rowCount) {
    ColumnVector input = children.get(0);
    checkArgument(input.getDataType() instanceof StructType, "TO_JSON requires a struct input");
    checkArgument(input.getSize() == rowCount, "TO_JSON input size mismatch");
    return new AbstractDelegatingColumnVector(input.getSize(), StringType.STRING) {
      @Override
      public void close() {
        input.close();
      }

      @Override
      protected ColumnVector delegateVector(int rowId) {
        return input;
      }

      @Override
      protected int delegateRowId(int rowId) {
        return rowId;
      }

      @Override
      public String getString(int rowId) {
        checkValidRowId(rowId);
        try {
          return JsonUtils.structVectorToJson(input, rowId);
        } catch (RuntimeException failure) {
          throw new IllegalArgumentException("Could not serialize struct as JSON", failure);
        }
      }
    };
  }
}
