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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.expressions.ScalarExpression;
import io.delta.kernel.types.DataType;
import java.util.List;

/** Type resolution and vector evaluation for an extensible scalar expression. */
interface ExpressionKernel {
  String name();

  default DataType expectedChildType(DataType expectedType, int childIndex) {
    return null;
  }

  DataType resolve(
      ScalarExpression expression, List<DataType> childTypes, DataType expectedType);

  /** The returned vector owns the child vectors when evaluation succeeds. */
  ColumnVector eval(
      ScalarExpression expression,
      List<ColumnVector> children,
      DataType outputType,
      int rowCount);
}
