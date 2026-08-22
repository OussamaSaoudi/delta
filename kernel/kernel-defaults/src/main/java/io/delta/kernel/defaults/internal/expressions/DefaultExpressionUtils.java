/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import static io.delta.kernel.defaults.internal.DefaultEngineErrors.unsupportedExpressionException;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.defaults.internal.data.vector.AbstractDelegatingColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntPredicate;

/** Utility methods used by the default expression evaluator. */
public final class DefaultExpressionUtils {
  private enum ArithmeticOperation {
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE
  }

  static final Comparator<BigDecimal> BIGDECIMAL_COMPARATOR = Comparator.naturalOrder();
  static final Comparator<byte[]> BINARY_COMPARTOR =
      (leftOp, rightOp) -> {
        int i = 0;
        while (i < leftOp.length && i < rightOp.length) {
          if (leftOp[i] != rightOp[i]) {
            return Byte.toUnsignedInt(leftOp[i]) - Byte.toUnsignedInt(rightOp[i]);
          }
          i++;
        }
        return Integer.compare(leftOp.length, rightOp.length);
      };
  static final Comparator<String> STRING_COMPARATOR = DefaultExpressionUtils::compareStrings;

  private DefaultExpressionUtils() {}

  private static int compareStrings(String left, String right) {
    int leftOffset = 0;
    int rightOffset = 0;
    while (leftOffset < left.length() && rightOffset < right.length()) {
      int leftCodePoint = left.codePointAt(leftOffset);
      int rightCodePoint = right.codePointAt(rightOffset);
      if (leftCodePoint != rightCodePoint) {
        return Integer.compare(leftCodePoint, rightCodePoint);
      }
      leftOffset += Character.charCount(leftCodePoint);
      rightOffset += Character.charCount(rightCodePoint);
    }
    return Integer.compare(left.length() - leftOffset, right.length() - rightOffset);
  }
  public static boolean supportsComparison(DataType type) {
    return type instanceof BooleanType
        || type instanceof ByteType
        || type instanceof ShortType
        || type instanceof IntegerType
        || type instanceof DateType
        || type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType
        || type instanceof FloatType
        || type instanceof DoubleType
        || type instanceof DecimalType
        || type instanceof StringType
        || type instanceof GeometryType
        || type instanceof GeographyType
        || type instanceof BinaryType;
  }

  /** Compares two non-null Kernel values using the default expression ordering. */
  public static int compare(DataType type, Object left, Object right) {
    requireNonNull(type, "type is null");
    requireNonNull(left, "left is null");
    requireNonNull(right, "right is null");
    if (type instanceof BooleanType) {
      return Boolean.compare((Boolean) left, (Boolean) right);
    } else if (type instanceof ByteType) {
      return Byte.compare(((Number) left).byteValue(), ((Number) right).byteValue());
    } else if (type instanceof ShortType) {
      return Short.compare(((Number) left).shortValue(), ((Number) right).shortValue());
    } else if (type instanceof IntegerType || type instanceof DateType) {
      return Integer.compare(((Number) left).intValue(), ((Number) right).intValue());
    } else if (type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType) {
      return Long.compare(((Number) left).longValue(), ((Number) right).longValue());
    } else if (type instanceof FloatType) {
      return Float.compare(((Number) left).floatValue(), ((Number) right).floatValue());
    } else if (type instanceof DoubleType) {
      return Double.compare(((Number) left).doubleValue(), ((Number) right).doubleValue());
    } else if (type instanceof DecimalType) {
      return BIGDECIMAL_COMPARATOR.compare((BigDecimal) left, (BigDecimal) right);
    } else if (type instanceof StringType
        || type instanceof GeometryType
        || type instanceof GeographyType) {
      return STRING_COMPARATOR.compare((String) left, (String) right);
    } else if (type instanceof BinaryType) {
      return BINARY_COMPARTOR.compare((byte[]) left, (byte[]) right);
    }
    throw new UnsupportedOperationException("No comparator available for data type: " + type);
  }

  /** Compares two non-null vector values without boxing primitive values. */
  public static int compare(
      DataType type, ColumnVector left, int leftRow, ColumnVector right, int rightRow) {
    if (type instanceof BooleanType) {
      return Boolean.compare(left.getBoolean(leftRow), right.getBoolean(rightRow));
    } else if (type instanceof ByteType) {
      return Byte.compare(left.getByte(leftRow), right.getByte(rightRow));
    } else if (type instanceof ShortType) {
      return Short.compare(left.getShort(leftRow), right.getShort(rightRow));
    } else if (type instanceof IntegerType || type instanceof DateType) {
      return Integer.compare(left.getInt(leftRow), right.getInt(rightRow));
    } else if (type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType) {
      return Long.compare(left.getLong(leftRow), right.getLong(rightRow));
    } else if (type instanceof FloatType) {
      return Float.compare(left.getFloat(leftRow), right.getFloat(rightRow));
    } else if (type instanceof DoubleType) {
      return Double.compare(left.getDouble(leftRow), right.getDouble(rightRow));
    } else if (type instanceof DecimalType) {
      return BIGDECIMAL_COMPARATOR.compare(
          left.getDecimal(leftRow), right.getDecimal(rightRow));
    } else if (type instanceof StringType
        || type instanceof GeometryType
        || type instanceof GeographyType) {
      return STRING_COMPARATOR.compare(left.getString(leftRow), right.getString(rightRow));
    } else if (type instanceof BinaryType) {
      return BINARY_COMPARTOR.compare(left.getBinary(leftRow), right.getBinary(rightRow));
    }
    throw new UnsupportedOperationException("No comparator available for data type: " + type);
  }
  /**
   * Utility method that calculates the nullability result from given two vectors. Result is null if
   * at least one side is a null.
   */
  static boolean[] evalNullability(ColumnVector left, ColumnVector right) {
    int numRows = left.getSize();
    boolean[] nullability = new boolean[numRows];
    for (int rowId = 0; rowId < numRows; rowId++) {
      nullability[rowId] = left.isNullAt(rowId) || right.isNullAt(rowId);
    }
    return nullability;
  }

  /**
   * Wraps a child vector as a boolean {@link ColumnVector} with the given value and nullability
   * accessors.
   */
  static ColumnVector booleanWrapperVector(
      ColumnVector childVector, IntPredicate valueAccessor, IntPredicate nullabilityAccessor) {

    return new ColumnVector() {

      @Override
      public DataType getDataType() {
        return BooleanType.BOOLEAN;
      }

      @Override
      public int getSize() {
        return childVector.getSize();
      }

      @Override
      public void close() {
        childVector.close();
      }

      @Override
      public boolean isNullAt(int rowId) {
        return nullabilityAccessor.test(rowId);
      }

      @Override
      public boolean getBoolean(int rowId) {
        return valueAccessor.test(rowId);
      }
    };
  }

  /**
   * Utility method for getting value comparator
   *
   * @param left
   * @param right
   * @param booleanComparator
   * @return
   */
  static IntPredicate getComparator(
      ColumnVector left, ColumnVector right, IntPredicate booleanComparator) {
    checkArgument(
        left.getSize() == right.getSize(), "Left and right operand have different vector sizes.");

    DataType type = left.getDataType();
    if (!supportsComparison(type)) {
      throw new UnsupportedOperationException(type + " can not be compared.");
    }
    return rowId -> booleanComparator.test(compare(type, left, rowId, right, rowId));
  }

  /**
   * Utility method to create a column vector that lazily evaluate the comparator ex. (ie. ==, >=,
   * <=......) for left and right column vector according to the natural ordering of numbers
   *
   * <p>Only primitive data types are supported.
   */
  static ColumnVector comparatorVector(
      ColumnVector left, ColumnVector right, IntPredicate booleanComparator) {
    IntPredicate vectorValueComparator = getComparator(left, right, booleanComparator);

    return new ColumnVector() {

      @Override
      public DataType getDataType() {
        return BooleanType.BOOLEAN;
      }

      @Override
      public void close() {
        Utils.closeCloseables(left, right);
      }

      @Override
      public int getSize() {
        return left.getSize();
      }

      @Override
      public boolean isNullAt(int rowId) {
        return left.isNullAt(rowId) || right.isNullAt(rowId);
      }

      @Override
      public boolean getBoolean(int rowId) {
        if (isNullAt(rowId)) {
          return false;
        }
        return vectorValueComparator.test(rowId);
      }
    };
  }

  /**
   * Utility method to create a null safe column vector that lazily evaluate the comparator ex. (ie.
   * <=>) for left and right column vector according to the natural ordering of numbers
   *
   * <p>Only primitive data types are supported.
   */
  static ColumnVector nullSafeComparatorVector(
      ColumnVector left, ColumnVector right, IntPredicate booleanComparator) {
    IntPredicate vectorValueComparator = getComparator(left, right, booleanComparator);
    return new ColumnVector() {
      @Override
      public DataType getDataType() {
        return BooleanType.BOOLEAN;
      }

      @Override
      public void close() {
        Utils.closeCloseables(left, right);
      }

      @Override
      public int getSize() {
        return left.getSize();
      }

      @Override
      public boolean isNullAt(int rowId) {
        // Nullsafe comparator can never return null
        return false;
      }

      /**
       * Null safe comparator follows the truth table in Comparison Operators part of following link
       * https://spark.apache.org/docs/latest/sql-ref-null-semantics.html
       *
       * <p>If either left or right is null, return false If both left and right is null, return
       * true else compare the non null value of left and right
       *
       * @param rowId
       * @return
       */
      @Override
      public boolean getBoolean(int rowId) {
        if (left.isNullAt(rowId) && right.isNullAt(rowId)) {
          return true;
        } else if (left.isNullAt(rowId) || right.isNullAt(rowId)) {
          return false;
        }
        return vectorValueComparator.test(rowId);
      }
    };
  }

  static Expression childAt(Expression expression, int index) {
    return expression.getChildren().get(index);
  }

  /**
   * Combines a list of column vectors using the precomputed selected child for each row.
   *
   * @param vectors List of ColumnVectors of the same data type with length >= 1
   * @param selectedChildren index of the vector to use for each row
   */
  static ColumnVector combinationVector(List<ColumnVector> vectors, int[] selectedChildren) {
    return new AbstractDelegatingColumnVector(
        vectors.get(0).getSize(), vectors.get(0).getDataType()) {
      private boolean closed;

      @Override
      public void close() {
        if (!closed) {
          closed = true;
          Utils.closeCloseables(vectors.toArray(new ColumnVector[0]));
        }
      }

      @Override
      protected ColumnVector delegateVector(int rowId) {
        return getVector(rowId);
      }

      @Override
      protected int delegateRowId(int rowId) {
        return rowId;
      }

      private ColumnVector getVector(int rowId) {
        return vectors.get(selectedChildren[rowId]);
      }
    };
  }

  /**
   * Creates a column vector that lazily evaluates an arithmetic operation between two column
   * vectors.
   *
   * <p>Only numeric data types are supported.
   *
   * @param left the left operand column vector
   * @param right the right operand column vector
   * @param operation the arithmetic operation to apply
   * @return a new column vector representing the result of the arithmetic operation
   */
  static ColumnVector arithmeticVector(ColumnVector left, ColumnVector right, String operation) {
    checkArgument(
        left.getSize() == right.getSize(), "Left and right operand have different vector sizes.");
    checkArgument(
        left.getDataType().equals(right.getDataType()),
        "Left and right operand have different data types.");
    ArithmeticOperation arithmeticOperation = ArithmeticOperation.valueOf(operation);
    return new ColumnVector() {
      @Override
      public DataType getDataType() {
        return left.getDataType();
      }

      @Override
      public int getSize() {
        return left.getSize();
      }

      @Override
      public void close() {
        Utils.closeCloseables(left, right);
      }

      @Override
      public boolean isNullAt(int rowId) {
        return left.isNullAt(rowId) || right.isNullAt(rowId);
      }

      @Override
      public byte getByte(int rowId) {
        return evalByte(arithmeticOperation, left.getByte(rowId), right.getByte(rowId));
      }

      @Override
      public short getShort(int rowId) {
        return evalShort(arithmeticOperation, left.getShort(rowId), right.getShort(rowId));
      }

      @Override
      public int getInt(int rowId) {
        return evalInt(arithmeticOperation, left.getInt(rowId), right.getInt(rowId));
      }

      @Override
      public long getLong(int rowId) {
        return evalLong(arithmeticOperation, left.getLong(rowId), right.getLong(rowId));
      }

      @Override
      public float getFloat(int rowId) {
        return evalFloat(arithmeticOperation, left.getFloat(rowId), right.getFloat(rowId));
      }

      @Override
      public double getDouble(int rowId) {
        return evalDouble(arithmeticOperation, left.getDouble(rowId), right.getDouble(rowId));
      }
    };
  }

  private static byte evalByte(ArithmeticOperation operation, byte left, byte right) {
    int result = evalInt(operation, left, right);
    if (result < Byte.MIN_VALUE || result > Byte.MAX_VALUE) {
      throw arithmeticOverflow(operation, ByteType.BYTE);
    }
    return (byte) result;
  }

  private static short evalShort(ArithmeticOperation operation, short left, short right) {
    int result = evalInt(operation, left, right);
    if (result < Short.MIN_VALUE || result > Short.MAX_VALUE) {
      throw arithmeticOverflow(operation, ShortType.SHORT);
    }
    return (short) result;
  }

  private static int evalInt(ArithmeticOperation operation, int left, int right) {
    switch (operation) {
      case ADD:
        return Math.addExact(left, right);
      case SUBTRACT:
        return Math.subtractExact(left, right);
      case MULTIPLY:
        return Math.multiplyExact(left, right);
      case DIVIDE:
        if (left == Integer.MIN_VALUE && right == -1) {
          throw arithmeticOverflow(operation, IntegerType.INTEGER);
        }
        return left / right;
      default:
        throw new IllegalStateException("Unexpected arithmetic operation: " + operation);
    }
  }

  private static long evalLong(ArithmeticOperation operation, long left, long right) {
    switch (operation) {
      case ADD:
        return Math.addExact(left, right);
      case SUBTRACT:
        return Math.subtractExact(left, right);
      case MULTIPLY:
        return Math.multiplyExact(left, right);
      case DIVIDE:
        if (left == Long.MIN_VALUE && right == -1) {
          throw arithmeticOverflow(operation, LongType.LONG);
        }
        return left / right;
      default:
        throw new IllegalStateException("Unexpected arithmetic operation: " + operation);
    }
  }

  private static float evalFloat(ArithmeticOperation operation, float left, float right) {
    switch (operation) {
      case ADD:
        return left + right;
      case SUBTRACT:
        return left - right;
      case MULTIPLY:
        return left * right;
      case DIVIDE:
        return left / right;
      default:
        throw new IllegalStateException("Unexpected arithmetic operation: " + operation);
    }
  }

  private static double evalDouble(ArithmeticOperation operation, double left, double right) {
    switch (operation) {
      case ADD:
        return left + right;
      case SUBTRACT:
        return left - right;
      case MULTIPLY:
        return left * right;
      case DIVIDE:
        return left / right;
      default:
        throw new IllegalStateException("Unexpected arithmetic operation: " + operation);
    }
  }

  private static ArithmeticException arithmeticOverflow(
      ArithmeticOperation operation, DataType dataType) {
    return new ArithmeticException(
        String.format(
            "Arithmetic overflow while evaluating %s for %s values", operation, dataType));
  }

  /**
   * Checks if the specific expression is an integer literal, throws {@link
   * UnsupportedOperationException} if not.
   *
   * @param expr, expression to be checked.
   * @param context string describing the context, used for constructing error message.
   * @param baseExpression expression whose evaluation triggers this check. Used for constructing
   *     error message.
   */
  static void checkIntegerLiteral(Expression expr, String context, Expression baseExpression) {
    if (!(expr instanceof Literal) || !IntegerType.INTEGER.equals(((Literal) expr).getDataType())) {
      throw unsupportedExpressionException(
          baseExpression, String.format("%s, expects an integral numeric", context));
    }
  }

  /**
   * Checks the argument count of an expression. throws {@code unsupportedExpressionException} if
   * argument count mismatched.
   */
  static void checkArgsCount(Expression expr, int expectedCount, String exprName, String context) {
    if (expr.getChildren().size() != expectedCount) {
      throw unsupportedExpressionException(
          expr, String.format("Invalid number of inputs of %s expression, %s", exprName, context));
    }
  }

  static void checkIsStringType(DataType dataType, Expression parentExpr, String errorMessage) {
    if (dataType instanceof StringType) {
      return;
    }
    throw unsupportedExpressionException(parentExpr, errorMessage);
  }

  static void checkIsLiteral(Expression expr, Expression parentExpr, String errorMessage) {
    if (!(expr instanceof Literal)) {
      throw unsupportedExpressionException(parentExpr, errorMessage);
    }
  }

  /**
   * Checks if the collation is `UTF8_BINARY`, since this is the only collation the default engine
   * can evaluate.
   */
  static void checkIsUTF8BinaryCollation(
      Predicate predicate, CollationIdentifier collationIdentifier) {
    if (!collationIdentifier.isSparkUTF8BinaryCollation()) {
      String msg =
          format(
              "Unsupported collation: \"%s\". Default Engine supports just" + " \"%s\" collation.",
              collationIdentifier, CollationIdentifier.SPARK_UTF8_BINARY);
      throw unsupportedExpressionException(predicate, msg);
    }
  }

  /**
   * Checks if the given expression is a null literal.
   *
   * @param expression The expression to check
   * @return true if the expression is a Literal with null value, false otherwise
   */
  static boolean isNullLiteral(Expression expression) {
    return expression instanceof Literal && ((Literal) expression).getValue() == null;
  }
}
