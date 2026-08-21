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

import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Common immutable payload for Parquet and JSON scan source operators. */
public abstract class FileScan implements Operator {
  private final List<ScanFile> files;
  private final List<String> fileConstantColumns;
  private final StructType schema;

  protected FileScan(List<ScanFile> files, List<String> fileConstantColumns, StructType schema) {
    requireNonNull(files, "files is null");
    requireNonNull(fileConstantColumns, "fileConstantColumns is null");
    this.schema = requireNonNull(schema, "schema is null");

    List<String> copiedNames = new ArrayList<>(fileConstantColumns.size());
    StructType constantsSchema = buildConstantsSchema(fileConstantColumns, copiedNames);
    List<ScanFile> copiedFiles = new ArrayList<>(files.size());
    for (int fileIndex = 0; fileIndex < files.size(); fileIndex++) {
      ScanFile file = requireNonNull(files.get(fileIndex), "scan file is null");
      PlanValidation.requireRow(
          file.getFileConstants(),
          constantsSchema,
          String.format("Scan file %s constants", fileIndex));
      copiedFiles.add(file);
    }
    this.files = Collections.unmodifiableList(copiedFiles);
    this.fileConstantColumns = Collections.unmodifiableList(copiedNames);
  }

  private StructType buildConstantsSchema(List<String> names, List<String> copiedNames) {
    Set<String> seen = new HashSet<>();
    List<StructField> fields = new ArrayList<>(names.size());
    for (int index = 0; index < names.size(); index++) {
      String name = requireNonNull(names.get(index), "file constant column is null");
      if (!seen.add(name)) {
        throw new IllegalArgumentException(
            "fileConstantColumns contains duplicate name `" + name + "`");
      }
      int fieldIndex = schema.indexOf(name);
      if (fieldIndex < 0) {
        throw new IllegalArgumentException(
            "File constant column `" + name + "` is not in the scan schema");
      }
      StructField field = schema.at(fieldIndex);
      if (field.isMetadataColumn()) {
        throw new IllegalArgumentException(
            "File constant column `" + name + "` is a metadata column");
      }
      copiedNames.add(name);
      fields.add(field);
    }
    return new StructType(fields);
  }

  public List<ScanFile> getFiles() {
    return files;
  }

  public List<String> getFileConstantColumns() {
    return fileConstantColumns;
  }

  public StructType getSchema() {
    return schema;
  }

  @Override
  public final StructType getOutputSchema(List<StructType> inputSchemas) {
    requireNonNull(inputSchemas, "inputSchemas is null");
    if (!inputSchemas.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "%s requires no inputs, got %s", getClass().getSimpleName(), inputSchemas.size()));
    }
    return schema;
  }
}
