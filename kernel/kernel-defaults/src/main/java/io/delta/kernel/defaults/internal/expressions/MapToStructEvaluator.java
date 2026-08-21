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

import static io.delta.kernel.internal.util.PartitionUtils.strictLiteralForPartitionValue;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultStructVector;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Evaluates a resolved {@code MapToStruct} using Kernel-native vectors. */
final class MapToStructEvaluator {
  static ColumnVector eval(ColumnVector maps, StructType outputType) {
    requireNonNull(maps, "maps is null");
    requireNonNull(outputType, "outputType is null");

    int rowCount = maps.getSize();
    Object[][] fieldValues = new Object[outputType.length()][rowCount];
    boolean[] nullMaps = new boolean[rowCount];
    Map<String, Integer> fieldOrdinals = new HashMap<>();
    for (int ordinal = 0; ordinal < outputType.length(); ordinal++) {
      fieldOrdinals.put(outputType.at(ordinal).getName(), ordinal);
    }
    String[] rawValues = new String[outputType.length()];
    int[] matchedRow = new int[outputType.length()];

    for (int rowId = 0; rowId < rowCount; rowId++) {
      if (maps.isNullAt(rowId)) {
        nullMaps[rowId] = true;
        continue;
      }
      decodeMap(
          maps.getMap(rowId), outputType, fieldOrdinals, fieldValues, rawValues, matchedRow, rowId);
    }

    ColumnVector[] fields = new ColumnVector[outputType.length()];
    for (int ordinal = 0; ordinal < outputType.length(); ordinal++) {
      StructField field = outputType.at(ordinal);
      if (!field.isNullable()) {
        for (int rowId = 0; rowId < rowCount; rowId++) {
          checkArgument(
              fieldValues[ordinal][rowId] != null || nullMaps[rowId],
              "Found unmasked null at row %s for non-nullable MapToStruct field %s",
              rowId,
              field.getName());
        }
      }
      fields[ordinal] = DefaultGenericVector.fromArray(field.getDataType(), fieldValues[ordinal]);
    }
    return new DefaultStructVector(rowCount, outputType, Optional.of(nullMaps), fields);
  }

  private static void decodeMap(
      MapValue map,
      StructType outputType,
      Map<String, Integer> fieldOrdinals,
      Object[][] fieldValues,
      String[] rawValues,
      int[] matchedRow,
      int rowId) {
    int rowMarker = rowId + 1;
    ColumnVector keys = map.getKeys();
    ColumnVector values = map.getValues();
    for (int entry = 0; entry < map.getSize(); entry++) {
      if (keys.isNullAt(entry)) {
        continue;
      }
      Integer ordinal = fieldOrdinals.get(keys.getString(entry));
      if (ordinal != null) {
        matchedRow[ordinal] = rowMarker;
        rawValues[ordinal] = values.isNullAt(entry) ? null : values.getString(entry);
      }
    }

    for (int ordinal = 0; ordinal < outputType.length(); ordinal++) {
      if (matchedRow[ordinal] == rowMarker && rawValues[ordinal] != null) {
        StructField field = outputType.at(ordinal);
        fieldValues[ordinal][rowId] = parseValue(field.getDataType(), rawValues[ordinal]);
      }
    }
  }

  private static Object parseValue(DataType dataType, String rawValue) {
    if (rawValue.isEmpty() && !(dataType instanceof StringType || dataType instanceof BinaryType)) {
      return null;
    }
    return strictLiteralForPartitionValue(dataType, rawValue).getValue();
  }

  private MapToStructEvaluator() {}
}
