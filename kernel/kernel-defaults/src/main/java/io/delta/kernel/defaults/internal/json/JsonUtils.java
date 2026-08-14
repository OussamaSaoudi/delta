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

package io.delta.kernel.defaults.internal.json;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.delta.kernel.data.*;
import io.delta.kernel.defaults.internal.data.DefaultJsonRow;
import io.delta.kernel.internal.util.TimestampUtils;
import io.delta.kernel.types.*;
import java.io.IOException;
import java.io.StringWriter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * Utilities method to serialize and deserialize {@link Row} objects with a limited set of data type
 * values.
 *
 * <p>The legacy {@link #rowToJson(Row)} path supports {@code boolean}, {@code byte}, {@code short},
 * {@code int}, {@code long}, {@code float}, {@code double}, {@code string}, {@code StructType},
 * {@code ArrayType}, and string-keyed {@code MapType}. The declarative-plan {@link
 * #structVectorToJson(ColumnVector, int)} path supports all Kernel plan value types.
 *
 * <p>At a high-level, the JSON serialization is similar to that of Jackson's {@link ObjectMapper}.
 */
public class JsonUtils {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final DateTimeFormatter TIMESTAMP_NTZ_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("uuuu-MM-dd'T'HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
          .toFormatter();

  static {
    OBJECT_MAPPER.registerModule(new SimpleModule().addSerializer(Row.class, new RowSerializer()));
  }

  private JsonUtils() {}

  /**
   * Converts a {@link Row} to a single line JSON string. This is currently used just in tests. Wll
   * be used as part of the refactoring planned in <a
   * href="https://github.com/delta-io/delta/issues/2929">#2929</a>
   *
   * @param row the row to convert
   * @return JSON string
   */
  public static String rowToJson(Row row) {
    try {
      return OBJECT_MAPPER.writeValueAsString(row);
    } catch (JsonProcessingException ex) {
      throw new RuntimeException("Could not serialize row object to JSON", ex);
    }
  }

  /**
   * Converts one non-null struct vector element to JSON using the declarative plan semantics.
   *
   * <p>Null struct and map fields are omitted, while null array elements are retained.
   */
  public static String structVectorToJson(ColumnVector vector, int rowId) {
    checkArgument(vector.getDataType() instanceof StructType, "expected a struct vector");
    checkArgument(!vector.isNullAt(rowId), "expected a non-null struct value");
    StringWriter output = new StringWriter();
    try (JsonGenerator generator = OBJECT_MAPPER.getFactory().createGenerator(output)) {
      RowSerializer.writeStruct(generator, vector, (StructType) vector.getDataType(), rowId, true);
    } catch (IOException failure) {
      throw new RuntimeException("Could not serialize struct vector to JSON", failure);
    }
    return output.toString();
  }

  /**
   * Converts a JSON string to a {@link Row}.
   *
   * @param json JSON string
   * @param schema to read the JSON according the schema
   * @return {@link Row} instance with given schema.
   */
  public static Row rowFromJson(String json, StructType schema) {
    try {
      final JsonNode jsonNode = OBJECT_MAPPER.readTree(json);
      return new DefaultJsonRow((ObjectNode) jsonNode, schema);
    } catch (JsonProcessingException ex) {
      throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex);
    }
  }

  public static class RowSerializer extends StdSerializer<Row> {
    public RowSerializer() {
      super(Row.class);
    }

    @Override
    public void serialize(Row row, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      writeRow(gen, row, row.getSchema());
    }

    private void writeRow(JsonGenerator gen, Row row, StructType schema) throws IOException {
      gen.writeStartObject();
      for (int columnOrdinal = 0; columnOrdinal < schema.length(); columnOrdinal++) {
        StructField field = schema.at(columnOrdinal);
        if (!row.isNullAt(columnOrdinal)) {
          gen.writeFieldName(field.getName());
          writeValue(gen, row, columnOrdinal, field.getDataType());
        }
      }
      gen.writeEndObject();
    }

    private static void writeStruct(
        JsonGenerator gen,
        ColumnVector vector,
        StructType type,
        int rowId,
        boolean omitNullMapValues)
        throws IOException {
      gen.writeStartObject();
      for (int columnOrdinal = 0; columnOrdinal < type.length(); columnOrdinal++) {
        StructField field = type.at(columnOrdinal);
        ColumnVector childVector = vector.getChild(columnOrdinal);
        if (!childVector.isNullAt(rowId)) {
          gen.writeFieldName(field.getName());
          writeValue(gen, childVector, rowId, field.getDataType(), omitNullMapValues);
        }
      }
      gen.writeEndObject();
    }

    private static void writeArrayValue(
        JsonGenerator gen, ArrayValue arrayValue, ArrayType arrayType, boolean omitNullMapValues)
        throws IOException {
      gen.writeStartArray();
      ColumnVector arrayElems = arrayValue.getElements();
      for (int i = 0; i < arrayValue.getSize(); i++) {
        if (arrayElems.isNullAt(i)) {
          // Jackson serializes the null values in the array, but not in the map
          gen.writeNull();
        } else {
          writeValue(
              gen, arrayValue.getElements(), i, arrayType.getElementType(), omitNullMapValues);
        }
      }
      gen.writeEndArray();
    }

    private static void writeMapValue(
        JsonGenerator gen, MapValue mapValue, MapType mapType, boolean omitNullMapValues)
        throws IOException {
      assertSupportedMapType(mapType);
      gen.writeStartObject();
      ColumnVector keys = mapValue.getKeys();
      ColumnVector values = mapValue.getValues();
      for (int i = 0; i < mapValue.getSize(); i++) {
        if (omitNullMapValues && values.isNullAt(i)) {
          continue;
        }
        gen.writeFieldName(keys.getString(i));
        if (!values.isNullAt(i)) {
          writeValue(gen, values, i, mapType.getValueType(), omitNullMapValues);
        } else {
          gen.writeNull();
        }
      }
      gen.writeEndObject();
    }

    private void writeValue(JsonGenerator gen, Row row, int columnOrdinal, DataType type)
        throws IOException {
      checkArgument(!row.isNullAt(columnOrdinal), "value should not be null");
      if (type instanceof BooleanType) {
        gen.writeBoolean(row.getBoolean(columnOrdinal));
      } else if (type instanceof ByteType) {
        gen.writeNumber(row.getByte(columnOrdinal));
      } else if (type instanceof ShortType) {
        gen.writeNumber(row.getShort(columnOrdinal));
      } else if (type instanceof IntegerType) {
        gen.writeNumber(row.getInt(columnOrdinal));
      } else if (type instanceof LongType) {
        gen.writeNumber(row.getLong(columnOrdinal));
      } else if (type instanceof FloatType) {
        gen.writeNumber(row.getFloat(columnOrdinal));
      } else if (type instanceof DoubleType) {
        gen.writeNumber(row.getDouble(columnOrdinal));
      } else if (type instanceof StringType) {
        gen.writeString(row.getString(columnOrdinal));
      } else if (type instanceof StructType) {
        writeRow(gen, row.getStruct(columnOrdinal), (StructType) type);
      } else if (type instanceof ArrayType) {
        writeArrayValue(gen, row.getArray(columnOrdinal), (ArrayType) type, false);
      } else if (type instanceof MapType) {
        writeMapValue(gen, row.getMap(columnOrdinal), (MapType) type, false);
      } else {
        // `binary` type is not supported according the Delta Protocol
        throw new UnsupportedOperationException("unsupported data type: " + type);
      }
    }

    private static void writeValue(
        JsonGenerator gen, ColumnVector vector, int rowId, DataType type, boolean omitNullMapValues)
        throws IOException {
      checkArgument(!vector.isNullAt(rowId), "value should not be null");
      if (type instanceof BooleanType) {
        gen.writeBoolean(vector.getBoolean(rowId));
      } else if (type instanceof ByteType) {
        gen.writeNumber(vector.getByte(rowId));
      } else if (type instanceof ShortType) {
        gen.writeNumber(vector.getShort(rowId));
      } else if (type instanceof IntegerType) {
        gen.writeNumber(vector.getInt(rowId));
      } else if (type instanceof LongType) {
        gen.writeNumber(vector.getLong(rowId));
      } else if (type instanceof IntervalYearMonthType) {
        gen.writeNumber(vector.getIntervalYearMonth(rowId));
      } else if (type instanceof IntervalDayTimeType) {
        gen.writeNumber(vector.getIntervalDayTime(rowId));
      } else if (type instanceof FloatType) {
        float value = vector.getFloat(rowId);
        if (Float.isFinite(value)) {
          gen.writeRawValue(formatFloating(value, true));
        } else {
          gen.writeNull();
        }
      } else if (type instanceof DoubleType) {
        double value = vector.getDouble(rowId);
        if (Double.isFinite(value)) {
          gen.writeRawValue(formatFloating(value, false));
        } else {
          gen.writeNull();
        }
      } else if (type instanceof DecimalType) {
        gen.writeNumber(vector.getDecimal(rowId));
      } else if (type instanceof StringType
          || type instanceof GeometryType
          || type instanceof GeographyType) {
        gen.writeString(vector.getString(rowId));
      } else if (type instanceof BinaryType) {
        gen.writeString(encodeHex(vector.getBinary(rowId)));
      } else if (type instanceof DateType) {
        gen.writeString(LocalDate.ofEpochDay(vector.getInt(rowId)).toString());
      } else if (type instanceof TimestampType) {
        gen.writeString(
            DateTimeFormatter.ISO_INSTANT.format(
                TimestampUtils.instantFromEpochMicros(vector.getLong(rowId))));
      } else if (type instanceof TimestampNTZType) {
        gen.writeString(formatTimestampNtz(vector.getLong(rowId)));
      } else if (type instanceof StructType) {
        writeStruct(gen, vector, (StructType) type, rowId, omitNullMapValues);
      } else if (type instanceof ArrayType) {
        writeArrayValue(gen, vector.getArray(rowId), (ArrayType) type, omitNullMapValues);
      } else if (type instanceof MapType) {
        writeMapValue(gen, vector.getMap(rowId), (MapType) type, omitNullMapValues);
      } else if (type instanceof VariantType) {
        VariantValue variant = vector.getVariant(rowId);
        gen.writeStartObject();
        gen.writeStringField("metadata", encodeHex(variant.getMetadata()));
        gen.writeStringField("value", encodeHex(variant.getValue()));
        gen.writeEndObject();
      } else if (type instanceof VoidType) {
        throw new IllegalArgumentException("Void values must be null");
      } else {
        throw new UnsupportedOperationException("unsupported data type: " + type);
      }
    }
  }

  private static void assertSupportedMapType(MapType keyType) {
    checkArgument(
        keyType.getKeyType() instanceof StringType,
        "Only STRING type keys are supported in MAP type in JSON serialization");
  }

  private static String formatTimestampNtz(long micros) {
    LocalDateTime value =
        LocalDateTime.ofEpochSecond(
            Math.floorDiv(micros, 1_000_000L),
            Math.toIntExact(Math.floorMod(micros, 1_000_000L) * 1_000),
            ZoneOffset.UTC);
    return TIMESTAMP_NTZ_FORMATTER.format(value);
  }

  /** Formats Java's shortest round-tripping digits with Arrow's display thresholds. */
  private static String formatFloating(double value, boolean singlePrecision) {
    String javaValue = singlePrecision ? Float.toString((float) value) : Double.toString(value);
    boolean negative = javaValue.charAt(0) == '-';
    String unsigned = negative ? javaValue.substring(1) : javaValue;
    int exponentMarker = Math.max(unsigned.indexOf('E'), unsigned.indexOf('e'));
    int exponent =
        exponentMarker < 0 ? 0 : Integer.parseInt(unsigned.substring(exponentMarker + 1));
    String decimal = exponentMarker < 0 ? unsigned : unsigned.substring(0, exponentMarker);
    int decimalPoint = decimal.indexOf('.');
    int point = (decimalPoint < 0 ? decimal.length() : decimalPoint) + exponent;
    String digits =
        decimalPoint < 0
            ? decimal
            : decimal.substring(0, decimalPoint) + decimal.substring(decimalPoint + 1);

    int leading = 0;
    while (leading < digits.length() - 1 && digits.charAt(leading) == '0') {
      leading++;
    }
    digits = digits.substring(leading);
    point -= leading;
    int trailing = digits.length();
    while (trailing > 1 && digits.charAt(trailing - 1) == '0') {
      trailing--;
    }
    digits = digits.substring(0, trailing);

    int length = digits.length();
    int decimalExponent = point - length;
    int upperFixed = singlePrecision ? 13 : 16;
    int lowerFixed = singlePrecision ? -6 : -5;
    StringBuilder result = new StringBuilder();
    if (negative) {
      result.append('-');
    }
    if (decimalExponent >= 0 && point <= upperFixed) {
      result.append(digits);
      for (int index = 0; index < decimalExponent; index++) {
        result.append('0');
      }
      result.append(".0");
    } else if (point > 0 && point <= upperFixed) {
      result.append(digits, 0, point).append('.').append(digits.substring(point));
    } else if (point > lowerFixed && point <= 0) {
      result.append("0.");
      for (int index = 0; index < -point; index++) {
        result.append('0');
      }
      result.append(digits);
    } else {
      result.append(digits.charAt(0));
      if (length > 1) {
        result.append('.').append(digits.substring(1));
      }
      result.append('e').append(point - 1);
    }
    return result.toString();
  }

  private static String encodeHex(byte[] bytes) {
    char[] chars = new char[bytes.length * 2];
    char[] digits = "0123456789abcdef".toCharArray();
    for (int index = 0; index < bytes.length; index++) {
      int value = Byte.toUnsignedInt(bytes[index]);
      chars[index * 2] = digits[value >>> 4];
      chars[index * 2 + 1] = digits[value & 0x0f];
    }
    return new String(chars);
  }
}
