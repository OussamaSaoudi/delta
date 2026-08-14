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
package io.delta.kernel.defaults.internal.data;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.defaults.internal.DefaultKernelUtils;
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;
import io.delta.kernel.internal.util.InternalUtils;
import io.delta.kernel.internal.util.TimestampUtils;
import io.delta.kernel.types.*;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Schema-directed JSON decoder used by the default {@code JsonHandler}. */
final class StrictJsonRowParser {
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  private StrictJsonRowParser() {}

  static Decoder forSchema(StructType schema) {
    return new Decoder(schema, containsDuplicateFieldNames(schema));
  }

  static final class Decoder {
    private final StructType schema;
    private final boolean useTreeDecoder;

    private Decoder(StructType schema, boolean useTreeDecoder) {
      this.schema = schema;
      this.useTreeDecoder = useTreeDecoder;
    }

    DefaultJsonRow parse(String json) throws IOException {
      if (useTreeDecoder) {
        return DefaultJsonRow.fromJsonTree(json, schema);
      }
      try (JsonParser parser = JSON_FACTORY.createParser(json)) {
        JsonToken token = parser.nextToken();
        if (token != JsonToken.START_OBJECT) {
          throw new IllegalArgumentException("Expected one JSON object");
        }
        return readStruct(parser, schema);
      }
    }

    boolean usesTreeDecoder() {
      return useTreeDecoder;
    }
  }

  /**
   * Reads a struct without constructing an intermediate JSON tree. Failures are delayed until the
   * closing token so duplicate fields retain Jackson's last-value-wins behavior.
   */
  private static DefaultJsonRow readStruct(JsonParser parser, StructType schema)
      throws IOException {
    Object[] values = new Object[schema.length()];
    boolean[] present = new boolean[schema.length()];
    RuntimeException[] failures = new RuntimeException[schema.length()];

    while (parser.nextToken() != JsonToken.END_OBJECT) {
      requireToken(parser, JsonToken.FIELD_NAME, "object field");
      int ordinal = schema.indexOf(parser.currentName());
      JsonToken valueToken = parser.nextToken();
      if (ordinal < 0) {
        parser.skipChildren();
        continue;
      }

      present[ordinal] = true;
      failures[ordinal] = null;
      try {
        values[ordinal] = readValue(parser, valueToken, schema.at(ordinal).getDataType());
      } catch (RuntimeException e) {
        values[ordinal] = null;
        failures[ordinal] = e;
      }
    }

    for (int ordinal = 0; ordinal < schema.length(); ordinal++) {
      StructField field = schema.at(ordinal);
      if (failures[ordinal] != null) {
        throw failures[ordinal];
      }
      if ((!present[ordinal] || values[ordinal] == null)
          && !field.isNullable()
          && !(field.getDataType() instanceof VoidType)) {
        throw new RuntimeException(
            String.format(
                "Decoded value at key %s is null but field isn't nullable", field.getName()));
      }
    }
    return new DefaultJsonRow(values, schema);
  }

  private static boolean containsDuplicateFieldNames(DataType type) {
    if (type instanceof StructType) {
      StructType struct = (StructType) type;
      for (int ordinal = 0; ordinal < struct.length(); ordinal++) {
        StructField field = struct.at(ordinal);
        for (int previous = 0; previous < ordinal; previous++) {
          if (field.getName().equals(struct.at(previous).getName())) {
            return true;
          }
        }
        if (containsDuplicateFieldNames(field.getDataType())) {
          return true;
        }
      }
    } else if (type instanceof ArrayType) {
      return containsDuplicateFieldNames(((ArrayType) type).getElementType());
    } else if (type instanceof MapType) {
      MapType map = (MapType) type;
      return containsDuplicateFieldNames(map.getKeyType())
          || containsDuplicateFieldNames(map.getValueType());
    }
    return false;
  }

  private static Object readValue(JsonParser parser, JsonToken token, DataType type)
      throws IOException {
    if (token == JsonToken.VALUE_NULL) {
      return null;
    }
    if (type instanceof VoidType) {
      throw mismatch(parser, "null");
    }
    if (type instanceof BooleanType) {
      requireScalar(parser, token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE);
      if (!token.isBoolean()) throw mismatch(parser, "boolean");
      return token == JsonToken.VALUE_TRUE;
    }
    if (type instanceof ByteType) {
      return (byte) readNarrowIntegral(parser, token, Byte.MIN_VALUE, Byte.MAX_VALUE, "byte");
    }
    if (type instanceof ShortType) {
      return (short) readNarrowIntegral(parser, token, Short.MIN_VALUE, Short.MAX_VALUE, "short");
    }
    if (type instanceof IntegerType) {
      return (int) readIntegral(parser, token, Integer.MIN_VALUE, Integer.MAX_VALUE, "integer");
    }
    if (type instanceof LongType) {
      return readIntegral(parser, token, Long.MIN_VALUE, Long.MAX_VALUE, "long");
    }
    if (type instanceof FloatType) {
      return readFloat(parser, token);
    }
    if (type instanceof DoubleType) {
      return readDouble(parser, token);
    }
    if (type instanceof StringType) {
      requireScalar(parser, token == JsonToken.VALUE_STRING);
      if (token != JsonToken.VALUE_STRING) throw mismatch(parser, "string");
      return parser.getText();
    }
    if (type instanceof DecimalType) {
      requireScalar(parser, token.isNumeric());
      if (!token.isNumeric()) throw mismatch(parser, "decimal");
      return normalizeDecimal(parser.getDecimalValue());
    }
    if (type instanceof DateType) {
      return InternalUtils.daysSinceEpoch(Date.valueOf(readString(parser, token, "date")));
    }
    if (type instanceof TimestampType) {
      Instant time = OffsetDateTime.parse(readString(parser, token, "timestamp")).toInstant();
      return TimestampUtils.toEpochMicros(time);
    }
    if (type instanceof TimestampNTZType) {
      return DefaultKernelUtils.parseTimestampNTZ(readString(parser, token, "timestamp_ntz"));
    }
    if (type instanceof StructType) {
      if (token != JsonToken.START_OBJECT) {
        parser.skipChildren();
        throw mismatch(parser, "object");
      }
      return readStruct(parser, (StructType) type);
    }
    if (type instanceof ArrayType) {
      return readArray(parser, token, (ArrayType) type);
    }
    if (type instanceof MapType) {
      return readMap(parser, token, (MapType) type);
    }
    if (type instanceof GeometryType || type instanceof GeographyType) {
      return readString(parser, token, "string");
    }
    parser.skipChildren();
    throw new UnsupportedOperationException("Unsupported DataType " + type);
  }

  private static long readNarrowIntegral(
      JsonParser parser, JsonToken token, long min, long max, String expected) throws IOException {
    if (token != JsonToken.VALUE_NUMBER_FLOAT) {
      return readIntegral(parser, token, min, max, expected);
    }
    try {
      long value = parser.getDecimalValue().longValueExact();
      if (value >= min && value <= max) {
        return value;
      }
    } catch (ArithmeticException ignored) {
      // Fall through to the standard type-mismatch error.
    }
    throw mismatch(parser, expected);
  }

  private static long readIntegral(
      JsonParser parser, JsonToken token, long min, long max, String expected) throws IOException {
    requireScalar(parser, token == JsonToken.VALUE_NUMBER_INT);
    if (token != JsonToken.VALUE_NUMBER_INT) throw mismatch(parser, expected);

    switch (parser.getNumberType()) {
      case INT:
      case LONG:
        long value = parser.getLongValue();
        if (value < min || value > max) {
          throw mismatch(parser, expected);
        }
        return value;
      case BIG_INTEGER:
        BigInteger big = parser.getBigIntegerValue();
        if (big.compareTo(BigInteger.valueOf(min)) < 0
            || big.compareTo(BigInteger.valueOf(max)) > 0) {
          throw mismatch(parser, expected);
        }
        return big.longValue();
      default:
        throw mismatch(parser, expected);
    }
  }

  private static float readFloat(JsonParser parser, JsonToken token) throws IOException {
    if (token.isNumeric()) {
      float value =
          token == JsonToken.VALUE_NUMBER_FLOAT
              ? normalizeDecimal(parser.getDecimalValue()).floatValue()
              : parser.getFloatValue();
      if (!Float.isInfinite(value)) return value;
    } else if (token == JsonToken.VALUE_STRING) {
      switch (parser.getText()) {
        case "NaN":
          return Float.NaN;
        case "+INF":
        case "+Infinity":
        case "Infinity":
          return Float.POSITIVE_INFINITY;
        case "-INF":
        case "-Infinity":
          return Float.NEGATIVE_INFINITY;
      }
    } else {
      parser.skipChildren();
    }
    throw mismatch(parser, "float");
  }

  private static double readDouble(JsonParser parser, JsonToken token) throws IOException {
    if (token.isNumeric()) {
      double value =
          token == JsonToken.VALUE_NUMBER_FLOAT
              ? normalizeDecimal(parser.getDecimalValue()).doubleValue()
              : parser.getDoubleValue();
      if (!Double.isInfinite(value)) return value;
    } else if (token == JsonToken.VALUE_STRING) {
      switch (parser.getText()) {
        case "NaN":
          return Double.NaN;
        case "+INF":
        case "+Infinity":
        case "Infinity":
          return Double.POSITIVE_INFINITY;
        case "-INF":
        case "-Infinity":
          return Double.NEGATIVE_INFINITY;
      }
    } else {
      parser.skipChildren();
    }
    throw mismatch(parser, "double");
  }

  private static String readString(JsonParser parser, JsonToken token, String expected)
      throws IOException {
    requireScalar(parser, token == JsonToken.VALUE_STRING);
    if (token != JsonToken.VALUE_STRING) throw mismatch(parser, expected);
    return parser.getText();
  }

  private static ArrayValue readArray(JsonParser parser, JsonToken token, ArrayType type)
      throws IOException {
    if (token != JsonToken.START_ARRAY) {
      parser.skipChildren();
      throw mismatch(parser, "array");
    }
    List<Object> elements = new ArrayList<>();
    RuntimeException failure = null;
    while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
      Object value = null;
      try {
        value = readValue(parser, token, type.getElementType());
        if (value == null && !type.containsNull() && !(type.getElementType() instanceof VoidType)) {
          throw new RuntimeException(
              "Array type expects no nulls as elements, but received `null` as array element");
        }
      } catch (RuntimeException e) {
        if (failure == null) failure = e;
      }
      elements.add(value);
    }
    if (failure != null) throw failure;
    Object[] values = elements.toArray();
    return new ArrayValue() {
      @Override
      public int getSize() {
        return values.length;
      }

      @Override
      public ColumnVector getElements() {
        return DefaultGenericVector.fromArray(type.getElementType(), values);
      }
    };
  }

  private static MapValue readMap(JsonParser parser, JsonToken token, MapType type)
      throws IOException {
    if (token != JsonToken.START_OBJECT) {
      parser.skipChildren();
      throw mismatch(parser, "map");
    }
    if (!(type.getKeyType() instanceof StringType)) {
      parser.skipChildren();
      throw new RuntimeException(
          "MapType with a key type of `String` is supported, received a key type: "
              + type.getKeyType());
    }

    LinkedHashMap<String, Object> values = new LinkedHashMap<>();
    Map<String, RuntimeException> failures = new LinkedHashMap<>();
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      requireToken(parser, JsonToken.FIELD_NAME, "map key");
      String key = parser.currentName();
      JsonToken valueToken = parser.nextToken();
      Object value = null;
      failures.remove(key);
      try {
        value =
            type.getValueType() instanceof StringType
                ? readMapString(parser, valueToken)
                : readValue(parser, valueToken, type.getValueType());
        if (value == null
            && !type.isValueContainsNull()
            && !(type.getValueType() instanceof VoidType)) {
          throw new RuntimeException(
              "Map type expects no nulls in values, but received `null` as value");
        }
      } catch (RuntimeException e) {
        failures.put(key, e);
      }
      values.put(key, value);
    }
    if (!failures.isEmpty()) throw failures.values().iterator().next();

    List<Object> keys = new ArrayList<>(values.keySet());
    List<Object> mapValues = new ArrayList<>(values.values());
    return new MapValue() {
      @Override
      public int getSize() {
        return keys.size();
      }

      @Override
      public ColumnVector getKeys() {
        return DefaultGenericVector.fromList(type.getKeyType(), keys);
      }

      @Override
      public ColumnVector getValues() {
        return DefaultGenericVector.fromList(type.getValueType(), mapValues);
      }
    };
  }

  /** Reproduces {@code JsonNode.asText()} for Delta's lenient map[string, string] values. */
  private static String readMapString(JsonParser parser, JsonToken token) throws IOException {
    if (token == JsonToken.VALUE_NULL) return null;
    if (token == JsonToken.VALUE_STRING) return parser.getText();
    if (token == JsonToken.VALUE_TRUE) return "true";
    if (token == JsonToken.VALUE_FALSE) return "false";
    if (token == JsonToken.VALUE_NUMBER_INT) return parser.getNumberValue().toString();
    if (token == JsonToken.VALUE_NUMBER_FLOAT) {
      return normalizeDecimal(parser.getDecimalValue()).toString();
    }
    parser.skipChildren();
    return "";
  }

  private static BigDecimal normalizeDecimal(BigDecimal value) {
    return value.signum() == 0 ? BigDecimal.ZERO : value.stripTrailingZeros();
  }

  private static void requireScalar(JsonParser parser, boolean valid) throws IOException {
    if (!valid) parser.skipChildren();
  }

  private static void requireToken(JsonParser parser, JsonToken expected, String description) {
    if (parser.currentToken() != expected) {
      throw new IllegalArgumentException("Expected " + description);
    }
  }

  private static RuntimeException mismatch(JsonParser parser, String expected) {
    String value;
    try {
      value = parser.getText();
    } catch (IOException e) {
      value = String.valueOf(parser.currentToken());
    }
    String expectedDescription = "null".equals(expected) ? expected : "a " + expected;
    return new RuntimeException(
        String.format("Couldn't decode %s, expected %s", value, expectedDescription));
  }
}
