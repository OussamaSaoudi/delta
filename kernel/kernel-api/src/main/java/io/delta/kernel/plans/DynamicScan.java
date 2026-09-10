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
package io.delta.kernel.plans;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Materializes file metadata rows and delegates them to the corresponding static scan. */
public final class DynamicScan extends PlanNode {
  private final PlanNode fileMetadataInput;
  private final StructType dataSchema;
  private final FileType fileType;
  private final URI tableRoot;
  private final List<String> fileConstantColumns;
  private final Column pathColumn;
  private final Column fileSizeColumn;
  private final Column lastModifiedColumn;
  private final Column deletionVectorColumn;
  private final List<PlanNode> children;

  public DynamicScan(
      PlanNode fileMetadataInput,
      StructType dataSchema,
      FileType fileType,
      URI tableRoot,
      List<String> fileConstantColumns,
      Column pathColumn,
      Column fileSizeColumn,
      Column lastModifiedColumn,
      Column deletionVectorColumn) {
    this.fileMetadataInput = requireNonNull(fileMetadataInput, "fileMetadataInput is null");
    this.dataSchema = requireNonNull(dataSchema, "dataSchema is null");
    this.fileType = requireNonNull(fileType, "fileType is null");
    this.tableRoot = requireNonNull(tableRoot, "tableRoot is null");
    if (!tableRoot.isAbsolute() || tableRoot.isOpaque() || !tableRoot.getPath().endsWith("/")) {
      throw new IllegalArgumentException(
          "Table root must be an absolute hierarchical URI ending in `/`: " + tableRoot);
    }
    this.fileConstantColumns = validateConstants(fileConstantColumns);
    this.pathColumn = requireNonNull(pathColumn, "pathColumn is null");
    this.fileSizeColumn = requireNonNull(fileSizeColumn, "fileSizeColumn is null");
    this.lastModifiedColumn = requireNonNull(lastModifiedColumn, "lastModifiedColumn is null");
    this.deletionVectorColumn =
        requireNonNull(deletionVectorColumn, "deletionVectorColumn is null");
    this.children = Collections.singletonList(fileMetadataInput);

    StructType metadataSchema = fileMetadataInput.outputSchema();
    PlanValidation.requireColumn(metadataSchema, pathColumn, StringType.STRING, "DynamicScan path");
    PlanValidation.requireColumn(
        metadataSchema, fileSizeColumn, LongType.LONG, "DynamicScan file size");
    PlanValidation.requireColumn(
        metadataSchema, lastModifiedColumn, LongType.LONG, "DynamicScan modification time");
    PlanValidation.requireColumn(
        metadataSchema,
        deletionVectorColumn,
        DeletionVectorDescriptor.READ_SCHEMA,
        "DynamicScan deletion vector");
    for (String name : this.fileConstantColumns) {
      StructField inputField = topLevelField(metadataSchema, name, "metadata");
      StructField outputField = topLevelField(dataSchema, name, "data");
      if (!inputField.getDataType().equals(outputField.getDataType())) {
        throw new IllegalArgumentException(
            "DynamicScan constant `" + name + "` has different metadata and data types");
      }
    }
  }

  private List<String> validateConstants(List<String> names) {
    List<String> copy = PlanValidation.immutableCopy(names, "file constant column is null");
    Set<String> seen = new HashSet<>();
    for (String name : copy) {
      if (!seen.add(name)) {
        throw new IllegalArgumentException("Duplicate file constant column `" + name + "`");
      }
    }
    return copy;
  }

  private static StructField topLevelField(StructType schema, String name, String owner) {
    int ordinal = schema.indexOf(name);
    if (ordinal < 0) {
      throw new IllegalArgumentException(
          "DynamicScan constant `" + name + "` is absent from the " + owner + " schema");
    }
    StructField field = schema.at(ordinal);
    if (field.isMetadataColumn()) {
      throw new IllegalArgumentException(
          "DynamicScan constant `" + name + "` is a metadata column in the " + owner + " schema");
    }
    return field;
  }

  public PlanNode fileMetadataInput() {
    return fileMetadataInput;
  }

  public StructType dataSchema() {
    return dataSchema;
  }

  public FileType fileType() {
    return fileType;
  }

  public URI tableRoot() {
    return tableRoot;
  }

  public List<String> fileConstantColumns() {
    return fileConstantColumns;
  }

  public Column pathColumn() {
    return pathColumn;
  }

  public Column fileSizeColumn() {
    return fileSizeColumn;
  }

  public Column lastModifiedColumn() {
    return lastModifiedColumn;
  }

  public Column deletionVectorColumn() {
    return deletionVectorColumn;
  }

  @Override
  public StructType outputSchema() {
    return dataSchema;
  }

  @Override
  public List<PlanNode> children() {
    return children;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof DynamicScan)) {
      return false;
    }
    DynamicScan that = (DynamicScan) other;
    return fileMetadataInput.equals(that.fileMetadataInput)
        && dataSchema.equals(that.dataSchema)
        && fileType == that.fileType
        && tableRoot.equals(that.tableRoot)
        && fileConstantColumns.equals(that.fileConstantColumns)
        && pathColumn.equals(that.pathColumn)
        && fileSizeColumn.equals(that.fileSizeColumn)
        && lastModifiedColumn.equals(that.lastModifiedColumn)
        && deletionVectorColumn.equals(that.deletionVectorColumn);
  }

  @Override
  public int hashCode() {
    if (hasMemoizedHashCode()) return memoizedHashCode();
    return memoizeHashCode(
        Objects.hash(
            fileMetadataInput,
            dataSchema,
            fileType,
            tableRoot,
            fileConstantColumns,
            pathColumn,
            fileSizeColumn,
            lastModifiedColumn,
            deletionVectorColumn));
  }
}
