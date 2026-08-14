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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

/** Kernel-native evaluation for {@code TO_JSON}. */
final class ToJsonExpressionEvaluator {
  private ToJsonExpressionEvaluator() {}

  static ColumnVector eval(ColumnVector input) {
    checkArgument(input.getDataType() instanceof StructType, "TO_JSON requires a struct input");
    Object[] values = new Object[input.getSize()];
    try {
      for (int rowId = 0; rowId < input.getSize(); rowId++) {
        if (!input.isNullAt(rowId)) {
          values[rowId] = JsonUtils.structVectorToJson(input, rowId);
        }
      }
      return DefaultGenericVector.fromArray(StringType.STRING, values);
    } catch (RuntimeException failure) {
      throw new IllegalArgumentException("Could not serialize struct as JSON", failure);
    } finally {
      input.close();
    }
  }
}
