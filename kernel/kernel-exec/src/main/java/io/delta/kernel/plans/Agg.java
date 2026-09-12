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
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** An aggregate function and its typed expression operands. */
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
  private final List<Expression> operands;
  private final List<DataType> operandTypes;

  private Agg(Function function, List<Expression> operands, List<DataType> operandTypes) {
    this.function = requireNonNull(function, "function is null");
    this.operands = immutableCopy(operands, "operand is null");
    this.operandTypes = immutableCopy(operandTypes, "operand type is null");
    if (this.operands.size() != this.operandTypes.size()) {
      throw new IllegalArgumentException("Aggregate operands and types have different arity");
    }
  }

  public static Agg min(Expression value, DataType valueType) {
    return unary(Function.MIN, value, valueType);
  }

  public static Agg max(Expression value, DataType valueType) {
    return unary(Function.MAX, value, valueType);
  }

  /** Returns a nullable LONG sum over INT or LONG inputs. */
  public static Agg sum(Expression value, DataType valueType) {
    requireLongInput(valueType, "SUM");
    return unary(Function.SUM, value, valueType);
  }

  /** Returns the non-null LONG count of non-null values. */
  public static Agg count(Expression value, DataType valueType) {
    return unary(Function.COUNT, value, valueType);
  }

  /** Returns the non-null LONG count of input rows. */
  public static Agg countStar() {
    return new Agg(Function.COUNT_STAR, Collections.emptyList(), Collections.emptyList());
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

  public Function function() {
    return function;
  }

  /** Returns value, or value/sentinel/key for a non-null-by aggregate. */
  public List<Expression> operands() {
    return operands;
  }

  /** Returns the types aligned with {@link #operands()}. */
  public List<DataType> operandTypes() {
    return operandTypes;
  }

  public DataType resultType() {
    switch (function) {
      case SUM:
      case COUNT:
      case COUNT_STAR:
        return LongType.LONG;
      default:
        return operandTypes.get(0);
    }
  }

  public boolean resultNullable() {
    return function != Function.COUNT && function != Function.COUNT_STAR;
  }

  private static Agg unary(Function function, Expression value, DataType valueType) {
    return new Agg(
        function,
        Collections.singletonList(requireNonNull(value, "value is null")),
        Collections.singletonList(requireNonNull(valueType, "valueType is null")));
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
        Arrays.asList(value, nullSentinel, key),
        Arrays.asList(valueType, nullSentinelType, keyType));
  }

  private static void requireLongInput(DataType type, String function) {
    requireNonNull(type, "valueType is null");
    if (!(type instanceof IntegerType) && !(type instanceof LongType)) {
      throw new IllegalArgumentException(
          function + " requires an INT or LONG input, found " + type);
    }
  }

  private static <T> List<T> immutableCopy(List<T> values, String nullMessage) {
    requireNonNull(values, "values is null");
    List<T> copy = new ArrayList<>(values.size());
    for (T value : values) {
      copy.add(requireNonNull(value, nullMessage));
    }
    return Collections.unmodifiableList(copy);
  }
}
