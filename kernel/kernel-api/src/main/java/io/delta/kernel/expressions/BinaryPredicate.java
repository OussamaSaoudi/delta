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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Evolving;

/** A binary predicate with the exact typing and operand shapes used by Kernel plans. */
@Evolving
public final class BinaryPredicate extends Predicate {
  public enum Operator {
    LESS_THAN("<"),
    GREATER_THAN(">"),
    EQUAL("="),
    DISTINCT("DISTINCT");

    private final String expressionName;

    Operator(String expressionName) {
      this.expressionName = expressionName;
    }
  }

  private final Operator operator;

  public BinaryPredicate(Operator operator, Expression left, Expression right) {
    super(
        requireNonNull(operator, "operator is null").expressionName,
        requireNonNull(left, "left is null"),
        requireNonNull(right, "right is null"));
    this.operator = operator;
  }

  public Operator getOperator() {
    return operator;
  }

  public Expression getLeft() {
    return getChildren().get(0);
  }

  public Expression getRight() {
    return getChildren().get(1);
  }
}
