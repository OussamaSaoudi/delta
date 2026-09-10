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

import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Common immutable payload for static JSON and Parquet scans. */
public abstract class FileScan extends PlanNode {
  private final List<ScanFile> files;
  private final Optional<URI> tableRoot;
  private final List<String> fileConstantColumns;
  private final StructType outputSchema;

  FileScan(
      List<ScanFile> files,
      Optional<URI> tableRoot,
      List<String> fileConstantColumns,
      StructType outputSchema) {
    this.files = PlanValidation.immutableCopy(files, "scan file is null");
    this.tableRoot = requireNonNull(tableRoot, "tableRoot is null");
    this.fileConstantColumns = validateConstants(fileConstantColumns, outputSchema);
    this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
    if (tableRoot.isPresent() && !tableRoot.get().isAbsolute()) {
      throw new IllegalArgumentException("Table root is not an absolute URI: " + tableRoot.get());
    }

    StructType constantsSchema = constantsSchema(outputSchema, this.fileConstantColumns);
    for (int index = 0; index < this.files.size(); index++) {
      ScanFile file = this.files.get(index);
      PlanValidation.requireRow(
          file.getFileConstants(), constantsSchema, "Scan file " + index + " constants");
      Optional<DeletionVectorDescriptor> dv = file.getDeletionVector();
      if (dv.isPresent()
          && DeletionVectorDescriptor.UUID_DV_MARKER.equals(dv.get().getStorageType())
          && !tableRoot.isPresent()) {
        throw new IllegalArgumentException("Table root is required for relative deletion vectors");
      }
    }
  }

  private static List<String> validateConstants(List<String> names, StructType outputSchema) {
    requireNonNull(outputSchema, "outputSchema is null");
    List<String> copy = PlanValidation.immutableCopy(names, "file constant column is null");
    Set<String> seen = new HashSet<>();
    for (String name : copy) {
      if (!seen.add(name)) {
        throw new IllegalArgumentException("Duplicate file constant column `" + name + "`");
      }
      int ordinal = outputSchema.indexOf(name);
      if (ordinal < 0) {
        throw new IllegalArgumentException(
            "File constant column `" + name + "` is absent from the scan schema");
      }
      if (outputSchema.at(ordinal).isMetadataColumn()) {
        throw new IllegalArgumentException(
            "File constant column `" + name + "` is a metadata column");
      }
    }
    return copy;
  }

  private static StructType constantsSchema(StructType schema, List<String> names) {
    List<StructField> fields = new ArrayList<>(names.size());
    for (String name : names) {
      fields.add(schema.get(name));
    }
    return new StructType(fields);
  }

  public List<ScanFile> files() {
    return files;
  }

  public Optional<URI> tableRoot() {
    return tableRoot;
  }

  public List<String> fileConstantColumns() {
    return fileConstantColumns;
  }

  @Override
  public final StructType outputSchema() {
    return outputSchema;
  }

  @Override
  public final List<PlanNode> children() {
    return Collections.emptyList();
  }
}
