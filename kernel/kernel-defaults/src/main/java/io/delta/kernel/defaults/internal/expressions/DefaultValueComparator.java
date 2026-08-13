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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.GeographyType;
import io.delta.kernel.types.GeometryType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/** Natural ordering shared by default-engine expressions and relational operators. */
public final class DefaultValueComparator {
  private DefaultValueComparator() {}

  public static boolean supports(DataType type) {
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
      return ((BigDecimal) left).compareTo((BigDecimal) right);
    } else if (type instanceof StringType
        || type instanceof GeometryType
        || type instanceof GeographyType) {
      return compareBytes(
          ((String) left).getBytes(StandardCharsets.UTF_8),
          ((String) right).getBytes(StandardCharsets.UTF_8));
    } else if (type instanceof BinaryType) {
      return compareBytes((byte[]) left, (byte[]) right);
    }
    throw new UnsupportedOperationException("No comparator available for data type: " + type);
  }

  static int compareBytes(byte[] left, byte[] right) {
    int length = Math.min(left.length, right.length);
    for (int index = 0; index < length; index++) {
      int comparison =
          Integer.compare(Byte.toUnsignedInt(left[index]), Byte.toUnsignedInt(right[index]));
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(left.length, right.length);
  }
}
