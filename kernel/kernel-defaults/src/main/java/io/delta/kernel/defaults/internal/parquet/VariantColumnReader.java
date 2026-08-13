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
package io.delta.kernel.defaults.internal.parquet;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.VariantValue;
import io.delta.kernel.defaults.internal.data.vector.DefaultVariantVector;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.BinaryType;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;

/** Materializes an unshredded Parquet Variant group as a Kernel Variant value. */
class VariantColumnReader extends GroupConverter implements ParquetColumnReaders.BaseColumnReader {
  private final ParquetColumnReaders.BinaryColumnReader valueReader;
  private final ParquetColumnReaders.BinaryColumnReader metadataReader;
  private final Converter[] converters;

  private boolean currentGroupStarted;
  private boolean currentValueSeen;
  private boolean currentMetadataSeen;

  VariantColumnReader(int initialBatchSize, GroupType typeFromFile) {
    checkArgument(initialBatchSize > 0, "invalid initialBatchSize: %s", initialBatchSize);
    ParquetSchemaUtils.validateUnshreddedVariant(typeFromFile, typeFromFile.getName());
    valueReader = new ParquetColumnReaders.BinaryColumnReader(BinaryType.BINARY, initialBatchSize);
    metadataReader =
        new ParquetColumnReaders.BinaryColumnReader(BinaryType.BINARY, initialBatchSize);
    converters = new Converter[typeFromFile.getFieldCount()];

    for (int index = 0; index < typeFromFile.getFieldCount(); index++) {
      String fieldName = typeFromFile.getType(index).getName();
      if ("value".equals(fieldName)) {
        converters[index] = binaryConverter(valueReader, true);
      } else if ("metadata".equals(fieldName)) {
        converters[index] = binaryConverter(metadataReader, false);
      } else {
        throw new IllegalArgumentException("Unexpected field in unshredded Variant: " + fieldName);
      }
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    checkArgument(!currentGroupStarted, "Variant group started more than once for one row");
    currentGroupStarted = true;
    currentValueSeen = false;
    currentMetadataSeen = false;
  }

  @Override
  public void end() {
    checkArgument(currentGroupStarted, "Variant group ended before it started");
    checkArgument(currentValueSeen, "Variant group is missing required field `value`");
    checkArgument(currentMetadataSeen, "Variant group is missing required field `metadata`");
    currentGroupStarted = false;
  }

  @Override
  public ColumnVector getDataColumnVector(int batchSize) {
    checkArgument(!currentGroupStarted, "Variant group has not ended");
    ColumnVector values = valueReader.getDataColumnVector(batchSize);
    ColumnVector metadata = metadataReader.getDataColumnVector(batchSize);
    try {
      VariantValue[] variants = new VariantValue[batchSize];
      for (int rowId = 0; rowId < batchSize; rowId++) {
        boolean valueIsNull = values.isNullAt(rowId);
        boolean metadataIsNull = metadata.isNullAt(rowId);
        checkArgument(
            valueIsNull == metadataIsNull,
            "Variant row %s must contain both `value` and `metadata`",
            rowId);
        if (!valueIsNull) {
          variants[rowId] = new VariantValue(values.getBinary(rowId), metadata.getBinary(rowId));
        }
      }
      return new DefaultVariantVector(batchSize, variants);
    } finally {
      Utils.closeCloseables(values, metadata);
    }
  }

  @Override
  public void finalizeCurrentRow(long fileRowIndex) {
    checkArgument(!currentGroupStarted, "Variant group was not completed for row %s", fileRowIndex);
    valueReader.finalizeCurrentRow(fileRowIndex);
    metadataReader.finalizeCurrentRow(fileRowIndex);
  }

  private PrimitiveConverter binaryConverter(
      ParquetColumnReaders.BinaryColumnReader reader, boolean isValue) {
    return new PrimitiveConverter() {
      @Override
      public void addBinary(Binary binary) {
        checkArgument(currentGroupStarted, "Variant child was read outside a Variant group");
        if (isValue) {
          checkArgument(!currentValueSeen, "Variant field `value` appeared more than once");
          currentValueSeen = true;
        } else {
          checkArgument(!currentMetadataSeen, "Variant field `metadata` appeared more than once");
          currentMetadataSeen = true;
        }
        reader.addBinary(binary);
      }
    };
  }
}
