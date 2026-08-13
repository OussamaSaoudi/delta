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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector;
import io.delta.kernel.expressions.BinaryPredicate;
import io.delta.kernel.expressions.Junction;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.*;
import java.util.List;
import java.util.Optional;

/** Evaluation kernels for the predicate nodes mirrored from Kernel Rust plans. */
final class PlanPredicateEvaluator {
  private PlanPredicateEvaluator() {}

  static ColumnVector junction(Junction.Operator operator, List<ColumnVector> children, int size) {
    boolean[] values = new boolean[size];
    boolean[] nullability = new boolean[size];
    try {
      for (ColumnVector child : children) {
        checkArgument(
            child.getDataType().equals(BooleanType.BOOLEAN), "junction child not boolean");
        checkArgument(child.getSize() == size, "junction child size mismatch");
      }
      for (int rowId = 0; rowId < size; rowId++) {
        boolean sawNull = false;
        boolean decisive = false;
        for (ColumnVector child : children) {
          if (child.isNullAt(rowId)) {
            sawNull = true;
          } else if (operator == Junction.Operator.AND && !child.getBoolean(rowId)) {
            decisive = true;
            break;
          } else if (operator == Junction.Operator.OR && child.getBoolean(rowId)) {
            decisive = true;
            values[rowId] = true;
            break;
          }
        }
        if (!decisive) {
          if (sawNull) {
            nullability[rowId] = true;
          } else {
            values[rowId] = operator == Junction.Operator.AND;
          }
        }
      }
      return new DefaultBooleanVector(size, Optional.of(nullability), values);
    } finally {
      Utils.closeCloseables(children.toArray(new ColumnVector[0]));
    }
  }

  static ColumnVector strictComparison(
      BinaryPredicate.Operator operator, ColumnVector left, ColumnVector right) {
    checkArgument(
        left.getDataType().equals(right.getDataType()),
        "comparison operand types must match exactly");
    switch (operator) {
      case LESS_THAN:
        return DefaultExpressionUtils.comparatorVector(left, right, value -> value < 0);
      case GREATER_THAN:
        return DefaultExpressionUtils.comparatorVector(left, right, value -> value > 0);
      case EQUAL:
        return DefaultExpressionUtils.comparatorVector(left, right, value -> value == 0);
      case DISTINCT:
        ColumnVector equal =
            DefaultExpressionUtils.nullSafeComparatorVector(left, right, value -> value == 0);
        return DefaultExpressionUtils.booleanWrapperVector(
            equal, rowId -> !equal.getBoolean(rowId), rowId -> false);
      default:
        throw new IllegalArgumentException("Unsupported binary predicate operator: " + operator);
    }
  }
}
