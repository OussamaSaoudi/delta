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

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector;
import io.delta.kernel.expressions.BinaryPredicate;
import io.delta.kernel.expressions.Junction;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.*;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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
        throw new IllegalArgumentException("IN is evaluated by inList");
    }
  }

  static ColumnVector inList(Literal literal, ColumnVector lists, boolean literalArray) {
    try {
      checkArgument(lists.getDataType() instanceof ArrayType, "IN right operand must be an array");
      ArrayType arrayType = (ArrayType) lists.getDataType();
      checkArgument(
          literal.getDataType().equals(arrayType.getElementType()),
          "IN operand and array element types must match exactly");
      boolean[] values = new boolean[lists.getSize()];
      if (literal.getValue() == null && !literalArray) {
        return new DefaultBooleanVector(lists.getSize(), Optional.empty(), values);
      }
      for (int rowId = 0; rowId < lists.getSize(); rowId++) {
        if (lists.isNullAt(rowId)) continue;
        ArrayValue list = lists.getArray(rowId);
        for (int index = 0; index < list.getSize(); index++) {
          if (valueEqualsVector(
              literal.getValue(), list.getElements(), index, literal.getDataType())) {
            values[rowId] = true;
            break;
          }
        }
      }
      return new DefaultBooleanVector(lists.getSize(), Optional.empty(), values);
    } finally {
      lists.close();
    }
  }

  static boolean supportsColumnInElementType(DataType type) {
    return type instanceof ByteType
        || type instanceof ShortType
        || type instanceof IntegerType
        || type instanceof LongType
        || type instanceof FloatType
        || type instanceof DoubleType
        || type instanceof DecimalType
        || type instanceof StringType
        || type instanceof DateType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType;
  }

  private static boolean valueEqualsVector(
      Object value, ColumnVector vector, int rowId, DataType type) {
    if (value == null || vector.isNullAt(rowId)) {
      return value == null && vector.isNullAt(rowId);
    }
    if (type instanceof StructType) {
      return rowEqualsVector((Row) value, vector, rowId, (StructType) type);
    }
    if (type instanceof ArrayType) {
      return arraysEqual((ArrayValue) value, vector.getArray(rowId), (ArrayType) type);
    }
    if (type instanceof MapType) {
      return mapsEqual((MapValue) value, vector.getMap(rowId), (MapType) type);
    }
    return primitiveEquals(value, readPrimitive(vector, rowId, type), type);
  }

  private static boolean vectorValuesEqual(
      ColumnVector left, ColumnVector right, int rowId, DataType type) {
    if (left.isNullAt(rowId) || right.isNullAt(rowId)) {
      return left.isNullAt(rowId) && right.isNullAt(rowId);
    }
    if (type instanceof StructType) {
      StructType structType = (StructType) type;
      for (int ordinal = 0; ordinal < structType.length(); ordinal++) {
        if (!vectorValuesEqual(
            left.getChild(ordinal),
            right.getChild(ordinal),
            rowId,
            structType.at(ordinal).getDataType())) {
          return false;
        }
      }
      return true;
    }
    if (type instanceof ArrayType) {
      return arraysEqual(left.getArray(rowId), right.getArray(rowId), (ArrayType) type);
    }
    if (type instanceof MapType) {
      return mapsEqual(left.getMap(rowId), right.getMap(rowId), (MapType) type);
    }
    return primitiveEquals(
        readPrimitive(left, rowId, type), readPrimitive(right, rowId, type), type);
  }

  private static boolean rowEqualsVector(
      Row row, ColumnVector vector, int rowId, StructType type) {
    for (int ordinal = 0; ordinal < type.length(); ordinal++) {
      DataType fieldType = type.at(ordinal).getDataType();
      ColumnVector fieldVector = vector.getChild(ordinal);
      if (row.isNullAt(ordinal) || fieldVector.isNullAt(rowId)) {
        if (row.isNullAt(ordinal) != fieldVector.isNullAt(rowId)) return false;
      } else if (fieldType instanceof StructType) {
        if (!rowEqualsVector(
            row.getStruct(ordinal), fieldVector, rowId, (StructType) fieldType)) return false;
      } else if (fieldType instanceof ArrayType) {
        if (!arraysEqual(
            row.getArray(ordinal), fieldVector.getArray(rowId), (ArrayType) fieldType)) {
          return false;
        }
      } else if (fieldType instanceof MapType) {
        if (!mapsEqual(
            row.getMap(ordinal), fieldVector.getMap(rowId), (MapType) fieldType)) return false;
      } else if (!primitiveEquals(
          readPrimitive(row, ordinal, fieldType),
          readPrimitive(fieldVector, rowId, fieldType),
          fieldType)) {
        return false;
      }
    }
    return true;
  }

  private static boolean arraysEqual(ArrayValue left, ArrayValue right, ArrayType type) {
    if (left.getSize() != right.getSize()) return false;
    for (int index = 0; index < left.getSize(); index++) {
      if (!vectorValuesEqual(
          left.getElements(), right.getElements(), index, type.getElementType())) return false;
    }
    return true;
  }

  private static boolean mapsEqual(MapValue left, MapValue right, MapType type) {
    if (left.getSize() != right.getSize()) return false;
    for (int index = 0; index < left.getSize(); index++) {
      if (!vectorValuesEqual(left.getKeys(), right.getKeys(), index, type.getKeyType())
          || !vectorValuesEqual(left.getValues(), right.getValues(), index, type.getValueType())) {
        return false;
      }
    }
    return true;
  }

  private static boolean primitiveEquals(Object left, Object right, DataType type) {
    if (type instanceof BinaryType) return Arrays.equals((byte[]) left, (byte[]) right);
    if (type instanceof FloatType) return (Float) left == (Float) right;
    if (type instanceof DoubleType) return (Double) left == (Double) right;
    return Objects.equals(left, right);
  }

  private static Object readPrimitive(ColumnVector vector, int rowId, DataType type) {
    if (type instanceof BooleanType) return vector.getBoolean(rowId);
    if (type instanceof ByteType) return vector.getByte(rowId);
    if (type instanceof ShortType) return vector.getShort(rowId);
    if (type instanceof IntegerType || type instanceof DateType) return vector.getInt(rowId);
    if (type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType) return vector.getLong(rowId);
    if (type instanceof FloatType) return vector.getFloat(rowId);
    if (type instanceof DoubleType) return vector.getDouble(rowId);
    if (type instanceof DecimalType) return vector.getDecimal(rowId);
    if (type instanceof StringType
        || type instanceof GeometryType
        || type instanceof GeographyType) return vector.getString(rowId);
    if (type instanceof BinaryType) return vector.getBinary(rowId);
    throw new UnsupportedOperationException(type + " cannot be compared for IN membership");
  }

  private static Object readPrimitive(Row row, int ordinal, DataType type) {
    if (type instanceof BooleanType) return row.getBoolean(ordinal);
    if (type instanceof ByteType) return row.getByte(ordinal);
    if (type instanceof ShortType) return row.getShort(ordinal);
    if (type instanceof IntegerType || type instanceof DateType) return row.getInt(ordinal);
    if (type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType) return row.getLong(ordinal);
    if (type instanceof FloatType) return row.getFloat(ordinal);
    if (type instanceof DoubleType) return row.getDouble(ordinal);
    if (type instanceof DecimalType) return row.getDecimal(ordinal);
    if (type instanceof StringType
        || type instanceof GeometryType
        || type instanceof GeographyType) return row.getString(ordinal);
    if (type instanceof BinaryType) return row.getBinary(ordinal);
    throw new UnsupportedOperationException(type + " cannot be compared for IN membership");
  }
}
