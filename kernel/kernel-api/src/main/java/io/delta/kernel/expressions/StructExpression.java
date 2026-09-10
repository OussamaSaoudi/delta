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
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Builds a struct value from one expression per output field.
 *
 * <p>The output field names, types, and nullability are supplied by the caller as the {@link
 * StructType} passed to the expression evaluator. The field expressions are matched to that schema
 * by ordinal.
 *
 * <p>An optional nullability predicate controls whether the whole struct exists for each input row.
 * A false or null predicate result makes the struct null; a true result makes it non-null.
 *
 * @since 4.4.0
 */
@Evolving
public final class StructExpression implements Expression {
  private final List<Expression> fieldExpressions;
  private final Optional<Expression> nullabilityPredicate;
  private final List<Expression> children;

  /** Creates an always non-null struct expression. */
  public StructExpression(List<Expression> fieldExpressions) {
    this(fieldExpressions, Optional.empty());
  }

  /** Creates a struct expression controlled by {@code nullabilityPredicate}. */
  public StructExpression(List<Expression> fieldExpressions, Expression nullabilityPredicate) {
    this(fieldExpressions, Optional.of(requireNonNull(nullabilityPredicate)));
  }

  private StructExpression(
      List<Expression> fieldExpressions, Optional<Expression> nullabilityPredicate) {
    requireNonNull(fieldExpressions, "fieldExpressions is null");
    List<Expression> fields = new ArrayList<>(fieldExpressions.size());
    for (int ordinal = 0; ordinal < fieldExpressions.size(); ordinal++) {
      fields.add(
          requireNonNull(
              fieldExpressions.get(ordinal),
              String.format("field expression at ordinal %d is null", ordinal)));
    }
    this.fieldExpressions = Collections.unmodifiableList(fields);
    this.nullabilityPredicate = requireNonNull(nullabilityPredicate);

    List<Expression> allChildren = new ArrayList<>(fields);
    this.nullabilityPredicate.ifPresent(allChildren::add);
    this.children = Collections.unmodifiableList(allChildren);
  }

  /** Returns the field expressions in output-schema order. */
  public List<Expression> getFieldExpressions() {
    return fieldExpressions;
  }

  /** Returns the optional expression controlling whole-struct nullability. */
  public Optional<Expression> getNullabilityPredicate() {
    return nullabilityPredicate;
  }

  @Override
  public List<Expression> getChildren() {
    return children;
  }

  @Override
  public String toString() {
    String fields =
        fieldExpressions.stream().map(Object::toString).collect(Collectors.joining(", "));
    return nullabilityPredicate
        .map(predicate -> String.format("STRUCT_IF(%s; %s)", predicate, fields))
        .orElseGet(() -> String.format("STRUCT(%s)", fields));
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof StructExpression)) {
      return false;
    }
    StructExpression that = (StructExpression) other;
    return fieldExpressions.equals(that.fieldExpressions)
        && nullabilityPredicate.equals(that.nullabilityPredicate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(StructExpression.class, fieldExpressions, nullabilityPredicate);
  }
}
