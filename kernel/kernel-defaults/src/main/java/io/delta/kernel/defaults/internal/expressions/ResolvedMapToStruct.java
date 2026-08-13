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

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.expressions.Expression;
import io.delta.kernel.types.StructType;
import java.util.List;

/** A {@code MapToStruct} whose evaluator-supplied output type has been resolved. */
final class ResolvedMapToStruct implements Expression {
  private final Expression mapExpression;
  private final StructType outputType;

  ResolvedMapToStruct(Expression mapExpression, StructType outputType) {
    this.mapExpression = requireNonNull(mapExpression, "mapExpression is null");
    this.outputType = requireNonNull(outputType, "outputType is null");
  }

  Expression getMapExpression() {
    return mapExpression;
  }

  StructType getOutputType() {
    return outputType;
  }

  @Override
  public List<Expression> getChildren() {
    return singletonList(mapExpression);
  }

  @Override
  public String toString() {
    return String.format("MAP_TO_STRUCT(%s)", mapExpression);
  }
}
