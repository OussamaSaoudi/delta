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
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.ScalarExpression;
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
    return eval(expression, outputType, null, rowCount, childEvaluator);
  }

  static ColumnVector eval(
      StructExpression expression,
      StructType outputType,
      StructType inputType,
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
      validateFieldNullability(result, expression, outputType, inputType, fields);
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
      EvaluatedStructVector structVector,
      StructExpression expression,
      StructType outputType,
      StructType inputType,
      ColumnVector[] fields) {
    for (int ordinal = 0; ordinal < fields.length; ordinal++) {
      StructField field = outputType.at(ordinal);
      if (field.isNullable()
          || isGuaranteedNonNull(
              expression.getFieldExpressions().get(ordinal),
              expression.getNullabilityPredicate(),
              inputType)) {
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

  private static boolean isGuaranteedNonNull(
      Expression field, Optional<Expression> structPredicate, StructType inputType) {
    if (inputType == null) {
      return false;
    }
    if (field instanceof Literal) {
      return ((Literal) field).getValue() != null;
    }
    if (isNullTest(field)) {
      return true;
    }
    Optional<Expression> mask = structPredicate.flatMap(StructExpressionEvaluator::nullTestChild);
    if (mask.isPresent() && equivalent(field, mask.get())) {
      return true;
    }
    if (field instanceof Column) {
      return columnGuaranteedByMask((Column) field, mask, inputType);
    }
    if (isCoalesce(field) && mask.filter(StructExpressionEvaluator::isCoalesce).isPresent()) {
      List<Expression> fields = field.getChildren();
      List<Expression> masks = mask.get().getChildren();
      if (fields.size() != masks.size()) {
        return false;
      }
      for (int index = 0; index < fields.size(); index++) {
        if (!(fields.get(index) instanceof Column)
            || !(masks.get(index) instanceof Column)
            || !columnGuaranteedByMask(
                (Column) fields.get(index), Optional.of(masks.get(index)), inputType)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private static boolean isNullTest(Expression expression) {
    if (!(expression instanceof Predicate)) {
      return false;
    }
    String name = ((Predicate) expression).getName().replace(' ', '_');
    return name.equalsIgnoreCase("IS_NULL") || name.equalsIgnoreCase("IS_NOT_NULL");
  }

  private static Optional<Expression> nullTestChild(Expression expression) {
    if (!(expression instanceof Predicate)
        || !((Predicate) expression).getName().replace(' ', '_').equalsIgnoreCase("IS_NOT_NULL")
        || expression.getChildren().size() != 1) {
      return Optional.empty();
    }
    return Optional.of(expression.getChildren().get(0));
  }

  private static boolean isCoalesce(Expression expression) {
    return expression instanceof ScalarExpression
        && ((ScalarExpression) expression).getName().equalsIgnoreCase("COALESCE");
  }

  private static boolean equivalent(Expression left, Expression right) {
    if (left instanceof Column && right instanceof Column) {
      return Arrays.equals(((Column) left).getNames(), ((Column) right).getNames());
    }
    if (isCoalesce(left) && isCoalesce(right)) {
      List<Expression> leftChildren = left.getChildren();
      List<Expression> rightChildren = right.getChildren();
      if (leftChildren.size() != rightChildren.size()) {
        return false;
      }
      for (int index = 0; index < leftChildren.size(); index++) {
        if (!equivalent(leftChildren.get(index), rightChildren.get(index))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private static boolean columnGuaranteedByMask(
      Column field, Optional<Expression> mask, StructType inputType) {
    List<PathStep> fieldPath = resolve(field, inputType);
    if (fieldPath == null || fieldPath.get(fieldPath.size() - 1).nullable) {
      return false;
    }
    List<PathStep> maskPath =
        mask.filter(Column.class::isInstance)
            .map(Column.class::cast)
            .map(column -> resolve(column, inputType))
            .orElse(null);
    for (int index = 0; index < fieldPath.size() - 1; index++) {
      PathStep step = fieldPath.get(index);
      // A non-null mask proves a nullable ancestor exists only when it follows the same path.
      if (step.nullable && !samePrefix(fieldPath, maskPath, index)) {
        return false;
      }
    }
    return true;
  }

  private static boolean samePrefix(List<PathStep> left, List<PathStep> right, int lastIndex) {
    if (right == null || right.size() <= lastIndex) {
      return false;
    }
    for (int index = 0; index <= lastIndex; index++) {
      if (!left.get(index).sameField(right.get(index))) {
        return false;
      }
    }
    return true;
  }

  private static List<PathStep> resolve(Column column, StructType inputType) {
    List<PathStep> path = new ArrayList<>();
    DataType current = inputType;
    for (String name : column.getNames()) {
      if (!(current instanceof StructType)) {
        return null;
      }
      StructType struct = (StructType) current;
      int ordinal = struct.indexOf(name);
      if (ordinal < 0) {
        return null;
      }
      StructField field = struct.at(ordinal);
      path.add(new PathStep(ordinal, field.getName(), field.isNullable()));
      current = field.getDataType();
    }
    return path.isEmpty() ? null : path;
  }

  private static final class PathStep {
    private final int ordinal;
    private final String name;
    private final boolean nullable;

    private PathStep(int ordinal, String name, boolean nullable) {
      this.ordinal = ordinal;
      this.name = name;
      this.nullable = nullable;
    }

    private boolean sameField(PathStep other) {
      return ordinal == other.ordinal && name.equals(other.name);
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
