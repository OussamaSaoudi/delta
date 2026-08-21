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
package io.delta.kernel.expressions;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Evolving;
import java.util.List;

/**
 * Converts a {@code map<string, string>} expression into a struct supplied as the evaluator's
 * output type.
 *
 * <p>Each map key matching an output field is parsed as that field's primitive type. Missing and
 * null values produce null fields, undeclared keys are ignored, and the rightmost duplicate key
 * wins. A null map produces a null struct. Empty strings remain empty for string and binary fields
 * and produce null for other field types.
 *
 * @since 4.4.0
 */
@Evolving
public final class MapToStruct implements Expression {
  private final Expression mapExpression;

  public MapToStruct(Expression mapExpression) {
    this.mapExpression = requireNonNull(mapExpression, "mapExpression is null");
  }

  /** Returns the expression producing {@code map<string, string>} values. */
  public Expression getMapExpression() {
    return mapExpression;
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
