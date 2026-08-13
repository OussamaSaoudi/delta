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
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultStructVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.StructExpression;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/** Evaluates {@link StructExpression}s into lazy Kernel-native struct vectors. */
final class StructExpressionEvaluator {
  @FunctionalInterface
  interface ChildEvaluator {
    ColumnVector eval(Expression expression, DataType expectedType);
  }

  static ColumnVector eval(
      StructExpression expression,
      StructType outputType,
      int rowCount,
      ChildEvaluator childEvaluator) {
    requireNonNull(expression, "expression is null");
    requireNonNull(outputType, "outputType is null");
    requireNonNull(childEvaluator, "childEvaluator is null");
    checkArgument(
        expression.getFieldExpressions().size() == outputType.length(),
        "Struct expression field count mismatch: %s fields in expression but %s in schema",
        expression.getFieldExpressions().size(),
        outputType.length());

    List<ColumnVector> fieldVectors = new ArrayList<>(outputType.length());
    ColumnVector nullabilityVector = null;
    try {
      for (int ordinal = 0; ordinal < outputType.length(); ordinal++) {
        StructField field = outputType.at(ordinal);
        ColumnVector vector =
            requireNonNull(
                childEvaluator.eval(
                    expression.getFieldExpressions().get(ordinal), field.getDataType()),
                String.format("field expression at ordinal %d returned null", ordinal));
        fieldVectors.add(vector);
        validateVector(vector, field.getDataType(), rowCount, "Struct field " + field.getName());
      }

      if (expression.getNullabilityPredicate().isPresent()) {
        nullabilityVector =
            requireNonNull(
                childEvaluator.eval(
                    expression.getNullabilityPredicate().get(), BooleanType.BOOLEAN),
                "nullability predicate returned null");
        validateVector(
            nullabilityVector, BooleanType.BOOLEAN, rowCount, "Struct nullability predicate");
      }

      ColumnVector[] fields = fieldVectors.toArray(new ColumnVector[0]);
      EvaluatedStructVector result =
          new EvaluatedStructVector(
              rowCount, outputType, fields, Optional.ofNullable(nullabilityVector));
      validateFieldNullability(result, outputType, fields);
      return result;
    } catch (RuntimeException failure) {
      closeAfterFailure(failure, fieldVectors, nullabilityVector);
      throw failure;
    }
  }

  private static void validateVector(
      ColumnVector vector, DataType expectedType, int expectedSize, String context) {
    checkArgument(
        expectedType.equals(vector.getDataType()),
        "%s type mismatch: expected %s but got %s",
        context,
        expectedType,
        vector.getDataType());
    checkArgument(
        vector.getSize() == expectedSize,
        "%s size mismatch: expected %s but got %s",
        context,
        expectedSize,
        vector.getSize());
  }

  private static void validateFieldNullability(
      EvaluatedStructVector structVector, StructType outputType, ColumnVector[] fields) {
    for (int ordinal = 0; ordinal < fields.length; ordinal++) {
      StructField field = outputType.at(ordinal);
      if (field.isNullable()) {
        continue;
      }
      for (int rowId = 0; rowId < structVector.getSize(); rowId++) {
        checkArgument(
            !fields[ordinal].isNullAt(rowId) || structVector.isNullAt(rowId),
            "Found unmasked null at row %s for non-nullable struct field %s",
            rowId,
            field.getName());
      }
    }
  }

  private static void closeAfterFailure(
      RuntimeException failure, List<ColumnVector> fieldVectors, ColumnVector nullabilityVector) {
    List<AutoCloseable> closeables = new ArrayList<>(fieldVectors);
    closeables.add(nullabilityVector);
    try {
      Utils.closeCloseables(closeables.toArray(new AutoCloseable[0]));
    } catch (RuntimeException closeFailure) {
      failure.addSuppressed(closeFailure);
    }
  }

  private static final class EvaluatedStructVector extends DefaultStructVector {
    private final Optional<ColumnVector> nullabilityVector;
    private final ColumnVector[] ownedFieldVectors;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private EvaluatedStructVector(
        int size,
        StructType dataType,
        ColumnVector[] fieldVectors,
        Optional<ColumnVector> nullabilityVector) {
      super(size, dataType, Optional.empty(), fieldVectors);
      this.ownedFieldVectors = fieldVectors;
      this.nullabilityVector = nullabilityVector;
    }

    @Override
    public boolean isNullAt(int rowId) {
      super.isNullAt(rowId); // Validate the row id.
      return nullabilityVector
          .map(vector -> vector.isNullAt(rowId) || !vector.getBoolean(rowId))
          .orElse(false);
    }

    @Override
    public void close() {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      List<AutoCloseable> closeables = new ArrayList<>();
      closeables.addAll(Arrays.asList(ownedFieldVectors));
      nullabilityVector.ifPresent(closeables::add);
      Utils.closeCloseables(closeables.toArray(new AutoCloseable[0]));
    }
  }

  private StructExpressionEvaluator() {}
}
