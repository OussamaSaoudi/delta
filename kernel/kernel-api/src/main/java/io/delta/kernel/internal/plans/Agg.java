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
package io.delta.kernel.internal.plans;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.Optional;

/** An aggregate function and its input columns. */
public final class Agg {
  /** Aggregate functions supported by Kernel plans. */
  public enum Function {
    MIN,
    MAX,
    MIN_NON_NULL_BY,
    MAX_NON_NULL_BY
  }

  private final Function function;
  private final Column value;
  private final Column key;

  private Agg(Function function, Column value, Column key) {
    this.function = requireNonNull(function, "function is null");
    this.value = requireNonNull(value, "value is null");
    this.key = key;
  }

  /** Returns the least non-null value, or null when there is no non-null value. */
  public static Agg min(Column value) {
    return new Agg(Function.MIN, value, null);
  }

  /** Returns the greatest non-null value, or null when there is no non-null value. */
  public static Agg max(Column value) {
    return new Agg(Function.MAX, value, null);
  }

  /** Returns the non-null value paired with the least non-null key. */
  public static Agg minNonNullBy(Column value, Column key) {
    return new Agg(Function.MIN_NON_NULL_BY, value, requireNonNull(key, "key is null"));
  }

  /** Returns the non-null value paired with the greatest non-null key. */
  public static Agg maxNonNullBy(Column value, Column key) {
    return new Agg(Function.MAX_NON_NULL_BY, value, requireNonNull(key, "key is null"));
  }

  StructField outputField(StructType inputSchema, String outputName) {
    if (key != null) {
      PlanValidation.resolveField(inputSchema, key, "Aggregate key");
    }
    StructField valueField = PlanValidation.resolveField(inputSchema, value, "Aggregate value");
    String name = outputName == null ? defaultOutputName() : outputName;
    return new StructField(
        name, PlanValidation.stripFieldMetadata(valueField.getDataType()), true);
  }

  private String defaultOutputName() {
    String[] names = value.getNames();
    if (names.length == 0) {
      throw new IllegalArgumentException(
          "Cannot derive an aggregate output name from an empty column path");
    }
    return names[names.length - 1];
  }

  public Function getFunction() {
    return function;
  }

  public Column getValue() {
    return value;
  }

  public Optional<Column> getKey() {
    return Optional.ofNullable(key);
  }
}
