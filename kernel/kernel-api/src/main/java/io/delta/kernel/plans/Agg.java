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
package io.delta.kernel.plans;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.expressions.Expression;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import java.util.Objects;
import java.util.Optional;

/** One aggregate function and its typed expression operands. */
public final class Agg {
  public enum Function {
    MIN,
    MAX,
    SUM,
    COUNT,
    COUNT_STAR,
    MIN_NON_NULL_BY,
    MAX_NON_NULL_BY
  }

  private final Function function;
  private final Expression value;
  private final DataType valueType;
  private final Expression nullSentinel;
  private final DataType nullSentinelType;
  private final Expression key;
  private final DataType keyType;

  private Agg(
      Function function,
      Expression value,
      DataType valueType,
      Expression nullSentinel,
      DataType nullSentinelType,
      Expression key,
      DataType keyType) {
    this.function = requireNonNull(function, "function is null");
    this.value = value;
    this.valueType = valueType;
    this.nullSentinel = nullSentinel;
    this.nullSentinelType = nullSentinelType;
    this.key = key;
    this.keyType = keyType;
  }

  public static Agg min(Expression value, DataType valueType) {
    return value(Function.MIN, value, valueType);
  }

  public static Agg max(Expression value, DataType valueType) {
    return value(Function.MAX, value, valueType);
  }

  /** A checked LONG sum. */
  public static Agg sum(Expression value) {
    return value(Function.SUM, value, LongType.LONG);
  }

  public static Agg count(Expression value, DataType valueType) {
    return value(Function.COUNT, value, valueType);
  }

  public static Agg countStar() {
    return new Agg(Function.COUNT_STAR, null, null, null, null, null, null);
  }

  public static Agg minNonNullBy(
      Expression value,
      DataType valueType,
      Expression nullSentinel,
      DataType nullSentinelType,
      Expression key,
      DataType keyType) {
    return nonNullBy(
        Function.MIN_NON_NULL_BY, value, valueType, nullSentinel, nullSentinelType, key, keyType);
  }

  public static Agg maxNonNullBy(
      Expression value,
      DataType valueType,
      Expression nullSentinel,
      DataType nullSentinelType,
      Expression key,
      DataType keyType) {
    return nonNullBy(
        Function.MAX_NON_NULL_BY, value, valueType, nullSentinel, nullSentinelType, key, keyType);
  }

  private static Agg value(Function function, Expression value, DataType valueType) {
    return new Agg(
        function,
        requireNonNull(value, "value is null"),
        requireNonNull(valueType, "valueType is null"),
        null,
        null,
        null,
        null);
  }

  private static Agg nonNullBy(
      Function function,
      Expression value,
      DataType valueType,
      Expression nullSentinel,
      DataType nullSentinelType,
      Expression key,
      DataType keyType) {
    return new Agg(
        function,
        requireNonNull(value, "value is null"),
        requireNonNull(valueType, "valueType is null"),
        requireNonNull(nullSentinel, "nullSentinel is null"),
        requireNonNull(nullSentinelType, "nullSentinelType is null"),
        requireNonNull(key, "key is null"),
        requireNonNull(keyType, "keyType is null"));
  }

  public Function function() {
    return function;
  }

  public Optional<Expression> value() {
    return Optional.ofNullable(value);
  }

  public Optional<DataType> valueType() {
    return Optional.ofNullable(valueType);
  }

  public Optional<Expression> nullSentinel() {
    return Optional.ofNullable(nullSentinel);
  }

  public Optional<DataType> nullSentinelType() {
    return Optional.ofNullable(nullSentinelType);
  }

  public Optional<Expression> key() {
    return Optional.ofNullable(key);
  }

  public Optional<DataType> keyType() {
    return Optional.ofNullable(keyType);
  }

  DataType outputType() {
    switch (function) {
      case SUM:
      case COUNT:
      case COUNT_STAR:
        return LongType.LONG;
      default:
        return valueType;
    }
  }

  boolean outputNullable() {
    return function != Function.COUNT && function != Function.COUNT_STAR;
  }

  void validateReferences(StructType inputSchema) {
    value()
        .ifPresent(
            expression ->
                PlanValidation.validateExpressionReferences(
                    inputSchema, expression, "Aggregate value"));
    nullSentinel()
        .ifPresent(
            expression ->
                PlanValidation.validateExpressionReferences(
                    inputSchema, expression, "Aggregate null sentinel"));
    key()
        .ifPresent(
            expression ->
                PlanValidation.validateExpressionReferences(
                    inputSchema, expression, "Aggregate key"));
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Agg)) {
      return false;
    }
    Agg that = (Agg) other;
    return function == that.function
        && Objects.equals(value, that.value)
        && Objects.equals(valueType, that.valueType)
        && Objects.equals(nullSentinel, that.nullSentinel)
        && Objects.equals(nullSentinelType, that.nullSentinelType)
        && Objects.equals(key, that.key)
        && Objects.equals(keyType, that.keyType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(function, value, valueType, nullSentinel, nullSentinelType, key, keyType);
  }
}
