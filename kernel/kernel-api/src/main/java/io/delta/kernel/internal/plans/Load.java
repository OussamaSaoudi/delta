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
package io.delta.kernel.internal.plans;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Reads files named by upstream metadata rows and emits rows matching a declared schema. */
public final class Load implements Operator {
  private final StructType schema;
  private final FileType fileType;
  private final Optional<URI> baseUri;
  private final List<String> fileConstantColumns;
  private final LoadColumnFileMeta fileMeta;
  private final Column dvColumn;

  public Load(StructType schema, FileType fileType, LoadColumnFileMeta fileMeta, Column dvColumn) {
    this(schema, fileType, Optional.empty(), Collections.emptyList(), fileMeta, dvColumn);
  }

  public Load(
      StructType schema,
      FileType fileType,
      Optional<URI> baseUri,
      List<String> fileConstantColumns,
      LoadColumnFileMeta fileMeta,
      Column dvColumn) {
    this.schema = requireNonNull(schema, "schema is null");
    this.fileType = requireNonNull(fileType, "fileType is null");
    this.baseUri = validateBaseUri(baseUri);
    this.fileConstantColumns = validateConstants(fileConstantColumns);
    this.fileMeta = requireNonNull(fileMeta, "fileMeta is null");
    this.dvColumn = requireNonNull(dvColumn, "dvColumn is null");
  }

  private Optional<URI> validateBaseUri(Optional<URI> value) {
    requireNonNull(value, "baseUri is null");
    if (value.isPresent() && !value.get().isAbsolute()) {
      throw new IllegalArgumentException("Load base URI is not absolute: " + value.get());
    }
    return value;
  }

  private List<String> validateConstants(List<String> names) {
    requireNonNull(names, "fileConstantColumns is null");
    List<String> copy = new ArrayList<>(names.size());
    Set<String> seen = new HashSet<>();
    for (String name : names) {
      requireNonNull(name, "file constant column is null");
      if (!seen.add(name)) {
        throw new IllegalArgumentException(
            "fileConstantColumns contains duplicate name `" + name + "`");
      }
      StructField field = topLevelField(schema, name, "output");
      requireDataField(field, name, "output");
      copy.add(name);
    }
    return Collections.unmodifiableList(copy);
  }

  @Override
  public StructType getOutputSchema(List<StructType> inputSchemas) {
    requireNonNull(inputSchemas, "inputSchemas is null");
    if (inputSchemas.size() != 1) {
      throw new IllegalArgumentException("Load requires one input, got " + inputSchemas.size());
    }
    StructType input = requireNonNull(inputSchemas.get(0), "input schema is null");
    requireColumn(input, fileMeta.getPathColumn(), StringType.STRING, false, "path");
    requireColumn(input, fileMeta.getFileSizeColumn(), LongType.LONG, true, "file size");
    requireColumn(input, fileMeta.getNumRecordsColumn(), LongType.LONG, true, "record count");
    requireColumn(input, dvColumn, DeletionVectorDescriptor.READ_SCHEMA, true, "deletion vector");

    for (String name : fileConstantColumns) {
      StructField source = topLevelField(input, name, "input");
      requireDataField(source, name, "input");
      StructField output = topLevelField(schema, name, "output");
      if (!source.getDataType().equals(output.getDataType())) {
        throw new IllegalArgumentException(
            String.format(
                "Load file constant `%s` has input type %s, but output type %s",
                name, source.getDataType(), output.getDataType()));
      }
    }
    return schema;
  }

  private static void requireColumn(
      StructType input,
      Column column,
      DataType expectedType,
      boolean expectedNullable,
      String label) {
    StructField field = PlanSchemaUtils.resolveField(input, column, "Load " + label);
    if (!expectedType.equals(field.getDataType()) || expectedNullable != field.isNullable()) {
      throw new IllegalArgumentException(
          String.format(
              "Load %s column %s must have type %s and nullable=%s, got %s and nullable=%s",
              label,
              column,
              expectedType,
              expectedNullable,
              field.getDataType(),
              field.isNullable()));
    }
  }

  private static StructField topLevelField(StructType owner, String name, String label) {
    int ordinal = owner.indexOf(name);
    if (ordinal < 0) {
      throw new IllegalArgumentException(
          String.format("Load file constant `%s` is absent from %s schema", name, label));
    }
    return owner.at(ordinal);
  }

  private static void requireDataField(StructField field, String name, String label) {
    if (field.isMetadataColumn()) {
      throw new IllegalArgumentException(
          String.format("Load file constant `%s` is a metadata column in %s schema", name, label));
    }
  }

  public StructType getSchema() {
    return schema;
  }

  public FileType getFileType() {
    return fileType;
  }

  public Optional<URI> getBaseUri() {
    return baseUri;
  }

  public List<String> getFileConstantColumns() {
    return fileConstantColumns;
  }

  public LoadColumnFileMeta getFileMeta() {
    return fileMeta;
  }

  public Column getDvColumn() {
    return dvColumn;
  }
}
