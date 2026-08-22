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
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.DefaultKernelUtils;
import io.delta.kernel.defaults.internal.data.vector.DefaultConstantVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultStructVector;
import io.delta.kernel.internal.util.InternalUtils;
import io.delta.kernel.internal.util.TimestampUtils;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.*;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Date;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        return readStruct(parser, schema, false);
      }
    }

    DefaultJsonRow parsePermissively(String json) throws IOException {
      if (useTreeDecoder) {
        return DefaultJsonRow.fromJsonTreePermissively(json, schema);
      }
      try (JsonParser parser = JSON_FACTORY.createParser(json)) {
        JsonToken token = parser.nextToken();
        if (token != JsonToken.START_OBJECT) {
          throw new IllegalArgumentException("Expected one JSON object");
        }
        DefaultJsonRow row = readStruct(parser, schema, true);
        if (parser.nextToken() != null) {
          throw new IllegalArgumentException("JSON input contains multiple values");
        }
        return row;
      }
    }

    boolean usesTreeDecoder() {
      return useTreeDecoder;
    }
  }

  static ColumnVector parsePermissiveBatch(ColumnVector input, StructType schema)
      throws IOException {
    if (containsDuplicateFieldNames(schema)) {
      Object[] rows = new Object[input.getSize()];
      Decoder decoder = forSchema(schema);
      for (int rowId = 0; rowId < input.getSize(); rowId++) {
        rows[rowId] =
            decoder.parsePermissively(input.isNullAt(rowId) ? "{}" : input.getString(rowId));
      }
      return DefaultGenericVector.fromArray(schema, rows);
    }

    StructColumns output = new StructColumns(schema, input.getSize(), false);
    JsonBatchReader reader = new JsonBatchReader(input);
    try (JsonParser parser = JSON_FACTORY.createParser(reader)) {
      requireToken(parser.nextToken(), JsonToken.START_ARRAY, "JSON batch");
      for (int rowId = 0; rowId < input.getSize(); rowId++) {
        requireToken(parser.nextToken(), JsonToken.START_ARRAY, "JSON row wrapper");
        requireToken(parser.nextToken(), JsonToken.START_OBJECT, "JSON object");
        output.read(parser, rowId, true);
        requireToken(parser.nextToken(), JsonToken.END_ARRAY, "JSON row wrapper");
        if (parser.getTokenLocation().getCharOffset() != reader.rowEndOffset(rowId)) {
          throw new IllegalArgumentException("JSON row crossed its input boundary");
        }
      }
      requireToken(parser.nextToken(), JsonToken.END_ARRAY, "JSON batch");
      if (parser.nextToken() != null) {
        throw new IllegalArgumentException("JSON batch contains trailing data");
      }
    }
    return output.build();
  }

  static ColumnarBatch parseStrictBatch(
      ColumnVector input, StructType schema, Optional<ColumnVector> selectionVector)
      throws IOException {
    ColumnVector selection = selectionVector.orElse(null);
    if (containsDuplicateFieldNames(schema)) {
      List<Row> rows = new ArrayList<>(input.getSize());
      Decoder decoder = forSchema(schema);
      for (int rowId = 0; rowId < input.getSize(); rowId++) {
        if (!isSelected(selection, rowId) || input.isNullAt(rowId)) {
          rows.add(null);
        } else {
          String json = input.getString(rowId);
          try {
            rows.add(decoder.parse(json));
          } catch (IOException failure) {
            throw parseFailure(json, failure);
          }
        }
      }
      return new DefaultRowBasedColumnarBatch(schema, rows);
    }

    StructColumns output = new StructColumns(schema, input.getSize(), false);
    for (int rowId = 0; rowId < input.getSize(); rowId++) {
      if (!isSelected(selection, rowId) || input.isNullAt(rowId)) {
        output.setNull(rowId);
        continue;
      }

      // Strict JsonHandler semantics intentionally accept the first object and ignore trailing
      // values. Keep one parser per row so batching does not accidentally tighten that contract.
      String json = input.getString(rowId);
      try (JsonParser parser = JSON_FACTORY.createParser(json)) {
        JsonToken token = parser.nextToken();
        if (token != JsonToken.START_OBJECT) {
          throw new IllegalArgumentException("Expected one JSON object");
        }
        output.read(parser, rowId, false);
      } catch (IOException failure) {
        throw parseFailure(json, failure);
      }
    }
    return output.buildBatch();
  }

  private static boolean isSelected(ColumnVector selectionVector, int rowId) {
    return selectionVector == null
        || (!selectionVector.isNullAt(rowId) && selectionVector.getBoolean(rowId));
  }

  private static IOException parseFailure(String json, IOException cause) {
    return new IOException("Could not parse JSON: " + json, cause);
  }

  private static final class StructColumns {
    private final StructType schema;
    private final int size;
    private final Object[][] values;
    private final StructColumns[] structs;
    private final boolean[] nulls;
    private final int[] seenGeneration;
    private final RuntimeException[] failures;
    private int generation;

    private StructColumns(StructType schema, int size, boolean nullable) {
      this.schema = schema;
      this.size = size;
      this.values = new Object[schema.length()][];
      this.structs = new StructColumns[schema.length()];
      this.nulls = nullable ? new boolean[size] : null;
      this.seenGeneration = new int[schema.length()];
      this.failures = new RuntimeException[schema.length()];
      for (int ordinal = 0; ordinal < schema.length(); ordinal++) {
        DataType type = schema.at(ordinal).getDataType();
        if (type instanceof StructType) {
          structs[ordinal] = new StructColumns((StructType) type, size, true);
        }
      }
    }

    private void read(JsonParser parser, int rowId, boolean nullFailureProneLeaves)
        throws IOException {
      if (nulls != null) nulls[rowId] = false;
      int currentGeneration = ++generation;
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        requireToken(parser, JsonToken.FIELD_NAME, "object field");
        int ordinal = schema.indexOf(parser.currentName());
        JsonToken valueToken = parser.nextToken();
        if (ordinal < 0) {
          parser.skipChildren();
          continue;
        }

        seenGeneration[ordinal] = currentGeneration;
        failures[ordinal] = null;
        DataType type = schema.at(ordinal).getDataType();
        try {
          if (type instanceof StructType) {
            readStructField(parser, valueToken, structs[ordinal], rowId, nullFailureProneLeaves);
          } else {
            Object value = readValue(parser, valueToken, type, nullFailureProneLeaves);
            if (value == null) {
              setNull(ordinal, rowId);
            } else {
              leafValues(ordinal)[rowId] = value;
            }
          }
        } catch (RuntimeException failure) {
          setNull(ordinal, rowId);
          if (!(nullFailureProneLeaves && DefaultJsonRow.isFailureProneLeaf(type))) {
            failures[ordinal] = failure;
          }
        }
      }

      for (int ordinal = 0; ordinal < schema.length(); ordinal++) {
        StructField field = schema.at(ordinal);
        if (seenGeneration[ordinal] != currentGeneration) {
          setNull(ordinal, rowId);
        } else if (failures[ordinal] != null) {
          throw failures[ordinal];
        }
        if (isNull(ordinal, rowId) && !field.isNullable()) {
          throw new IllegalArgumentException(
              "Null value for non-nullable field " + field.getName());
        }
      }
    }

    private static void readStructField(
        JsonParser parser,
        JsonToken token,
        StructColumns child,
        int rowId,
        boolean nullFailureProneLeaves)
        throws IOException {
      if (token == JsonToken.VALUE_NULL) {
        child.setNull(rowId);
      } else {
        if (token != JsonToken.START_OBJECT) {
          parser.skipChildren();
          throw mismatch(parser, "object");
        }
        child.read(parser, rowId, nullFailureProneLeaves);
      }
    }

    private void setNull(int ordinal, int rowId) {
      if (structs[ordinal] == null) {
        if (values[ordinal] != null) values[ordinal][rowId] = null;
      } else {
        structs[ordinal].setNull(rowId);
      }
    }

    private boolean isNull(int ordinal, int rowId) {
      return structs[ordinal] == null
          ? values[ordinal] == null || values[ordinal][rowId] == null
          : structs[ordinal].nulls[rowId];
    }

    private Object[] leafValues(int ordinal) {
      Object[] column = values[ordinal];
      if (column == null) {
        column = new Object[size];
        values[ordinal] = column;
      }
      return column;
    }

    private void setNull(int rowId) {
      if (nulls != null) nulls[rowId] = true;
      for (int ordinal = 0; ordinal < schema.length(); ordinal++) {
        setNull(ordinal, rowId);
      }
    }

    private ColumnVector[] buildChildren() {
      ColumnVector[] children = new ColumnVector[schema.length()];
      for (int ordinal = 0; ordinal < schema.length(); ordinal++) {
        children[ordinal] =
            structs[ordinal] == null ? buildLeaf(ordinal) : structs[ordinal].build();
      }
      return children;
    }

    private ColumnVector buildLeaf(int ordinal) {
      DataType type = schema.at(ordinal).getDataType();
      return values[ordinal] == null
          ? new DefaultConstantVector(type, size, null)
          : DefaultGenericVector.fromArray(type, values[ordinal]);
    }

    private ColumnVector build() {
      Optional<boolean[]> nullability = nulls == null ? Optional.empty() : Optional.of(nulls);
      return new OwnedStructVector(size, schema, nullability, buildChildren());
    }

    private ColumnarBatch buildBatch() {
      return new DefaultColumnarBatch(size, schema, buildChildren());
    }
  }

  private static final class OwnedStructVector extends DefaultStructVector {
    private final ColumnVector[] children;
    private boolean closed;

    private OwnedStructVector(
        int size, StructType schema, Optional<boolean[]> nullability, ColumnVector[] children) {
      super(size, schema, nullability, children);
      this.children = children;
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        Utils.closeCloseables(children);
      }
    }
  }

  /** Presents a string vector as one JSON array while preserving each row boundary. */
  private static final class JsonBatchReader extends Reader {
    private static final int OUTER_START = 0;
    private static final int ROW_START = 1;
    private static final int ROW_CONTENT = 2;
    private static final int ROW_END = 3;
    private static final int OUTER_END = 4;
    private static final int FINISHED = 5;

    private final ColumnVector input;
    private final long[] rowEndOffsets;
    private int phase = OUTER_START;
    private int rowId;
    private String segment;
    private int segmentOffset;
    private long streamOffset;
    private boolean closed;

    private JsonBatchReader(ColumnVector input) {
      this.input = input;
      this.rowEndOffsets = new long[input.getSize()];
    }

    @Override
    public int read(char[] buffer, int offset, int length) throws IOException {
      if (closed) throw new IOException("Reader is closed");
      if (buffer == null) throw new NullPointerException("buffer is null");
      if (offset < 0 || length < 0 || offset > buffer.length - length) {
        throw new IndexOutOfBoundsException();
      }
      if (length == 0) return 0;

      int written = 0;
      while (written < length) {
        if (segment == null || segmentOffset == segment.length()) {
          if (!advance()) return written == 0 ? -1 : written;
        }
        int count = Math.min(length - written, segment.length() - segmentOffset);
        segment.getChars(segmentOffset, segmentOffset + count, buffer, offset + written);
        segmentOffset += count;
        streamOffset += count;
        written += count;
      }
      return written;
    }

    private boolean advance() {
      segment = null;
      segmentOffset = 0;
      while (segment == null) {
        switch (phase) {
          case OUTER_START:
            segment = "[";
            phase = ROW_START;
            break;
          case ROW_START:
            if (rowId == input.getSize()) phase = OUTER_END;
            else {
              segment = rowId == 0 ? "[" : ",[";
              phase = ROW_CONTENT;
            }
            break;
          case ROW_CONTENT:
            segment = input.isNullAt(rowId) ? "{}" : input.getString(rowId);
            phase = ROW_END;
            break;
          case ROW_END:
            rowEndOffsets[rowId] = streamOffset;
            segment = "]";
            rowId++;
            phase = ROW_START;
            break;
          case OUTER_END:
            segment = "]";
            phase = FINISHED;
            break;
          case FINISHED:
            return false;
          default:
            throw new IllegalStateException("Unknown JSON batch reader phase: " + phase);
        }
      }
      return true;
    }

    private long rowEndOffset(int requestedRowId) {
      return rowEndOffsets[requestedRowId];
    }

    @Override
    public void close() {
      closed = true;
      segment = null;
    }
  }

  /**
   * Reads a struct without constructing an intermediate JSON tree. Failures are delayed until the
   * closing token so duplicate fields retain Jackson's last-value-wins behavior.
   */
  private static DefaultJsonRow readStruct(
      JsonParser parser, StructType schema, boolean nullFailureProneLeaves) throws IOException {
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
        values[ordinal] =
            readValue(parser, valueToken, schema.at(ordinal).getDataType(), nullFailureProneLeaves);
      } catch (RuntimeException e) {
        values[ordinal] = null;
        failures[ordinal] =
            nullFailureProneLeaves
                    && DefaultJsonRow.isFailureProneLeaf(schema.at(ordinal).getDataType())
                ? null
                : e;
      }
    }

    for (int ordinal = 0; ordinal < schema.length(); ordinal++) {
      StructField field = schema.at(ordinal);
      if (failures[ordinal] != null) {
        throw failures[ordinal];
      }
      if ((!present[ordinal] || values[ordinal] == null) && !field.isNullable()) {
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
        if (struct.indexOf(field.getName()) != ordinal) {
          return true;
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

  private static Object readValue(
      JsonParser parser, JsonToken token, DataType type, boolean nullFailureProneLeaves)
      throws IOException {
    if (token == JsonToken.VALUE_NULL) {
      return null;
    }
    if (type instanceof BooleanType) {
      requireScalar(parser, token.isBoolean(), "boolean");
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
      requireScalar(parser, token == JsonToken.VALUE_STRING, "string");
      return parser.getText();
    }
    if (type instanceof DecimalType) {
      boolean valid =
          token.isNumeric() || (nullFailureProneLeaves && token == JsonToken.VALUE_STRING);
      requireScalar(parser, valid, "decimal");
      BigDecimal value =
          token == JsonToken.VALUE_STRING
              ? new BigDecimal(parser.getText())
              : parser.getDecimalValue();
      if (!nullFailureProneLeaves) return normalizeDecimal(value);
      DecimalType decimalType = (DecimalType) type;
      BigDecimal scaled = value.setScale(decimalType.getScale(), RoundingMode.HALF_UP);
      if (scaled.precision() > decimalType.getPrecision()) {
        throw new ArithmeticException("Decimal exceeds precision " + decimalType.getPrecision());
      }
      return scaled;
    }
    if (type instanceof DateType) {
      return InternalUtils.daysSinceEpoch(Date.valueOf(readString(parser, token, "date")));
    }
    if (type instanceof TimestampType) {
      OffsetDateTime timestamp = OffsetDateTime.parse(readString(parser, token, "timestamp"));
      if (nullFailureProneLeaves && (timestamp.getYear() < 1 || timestamp.getYear() > 9999)) {
        throw mismatch(parser, "timestamp");
      }
      Instant time = timestamp.toInstant();
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
      return readStruct(parser, (StructType) type, nullFailureProneLeaves);
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
    requireScalar(parser, token == JsonToken.VALUE_NUMBER_INT, expected);

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
    requireScalar(parser, token == JsonToken.VALUE_STRING, expected);
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
        value = readValue(parser, token, type.getElementType(), false);
        if (value == null && !type.containsNull()) {
          throw new RuntimeException(
              "Array type expects no nulls as elements, but received `null` as array element");
        }
      } catch (RuntimeException e) {
        if (failure == null) failure = e;
      }
      elements.add(value);
    }
    if (failure != null) throw failure;
    return DefaultJsonRow.arrayValue(type.getElementType(), elements.toArray());
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
                : readValue(parser, valueToken, type.getValueType(), false);
        if (value == null && !type.isValueContainsNull()) {
          throw new RuntimeException(
              "Map type expects no nulls in values, but received `null` as value");
        }
      } catch (RuntimeException e) {
        failures.put(key, e);
      }
      values.put(key, value);
    }
    if (!failures.isEmpty()) throw failures.values().iterator().next();

    return DefaultJsonRow.mapValue(
        type, new ArrayList<>(values.keySet()), new ArrayList<>(values.values()));
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

  private static void requireScalar(JsonParser parser, boolean valid, String expected)
      throws IOException {
    if (!valid) {
      parser.skipChildren();
      throw mismatch(parser, expected);
    }
  }

  private static void requireToken(JsonParser parser, JsonToken expected, String description) {
    if (parser.currentToken() != expected) {
      throw new IllegalArgumentException("Expected " + description);
    }
  }

  private static void requireToken(JsonToken actual, JsonToken expected, String description) {
    if (actual != expected) {
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
