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

import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.util.RowKernels;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * One immutable operation in Kernel's closed execution plan language.
 *
 * <p>The private constructor prevents node kinds outside this file. Only {@link FileScan}, the
 * cache key, has structural equality.
 */
public abstract class PlanNode {
  private final StructType outputSchema;

  private PlanNode(StructType outputSchema) {
    this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
  }

  public final StructType outputSchema() {
    return outputSchema;
  }

  /** Keeps rows matching one engine-evaluated predicate. */
  public static final class Filter extends PlanNode {
    private final PlanNode input;
    private final Predicate predicate;

    public Filter(PlanNode input, Predicate predicate) {
      super(requireNonNull(input, "input is null").outputSchema());
      this.input = input;
      this.predicate = requireNonNull(predicate, "predicate is null");
    }

    public PlanNode input() {
      return input;
    }

    public Predicate predicate() {
      return predicate;
    }
  }

  /** Projects one input through one struct-valued expression. */
  public static final class Project extends PlanNode {
    private final PlanNode input;
    private final Expression rowExpression;

    public Project(PlanNode input, Expression rowExpression, StructType outputSchema) {
      super(outputSchema);
      requireNonNull(input, "input is null");
      this.input = input;
      this.rowExpression = requireNonNull(rowExpression, "rowExpression is null");
    }

    public PlanNode input() {
      return input;
    }

    public Expression rowExpression() {
      return rowExpression;
    }
  }

  /** Bag union whose inputs have one exact schema. */
  public static final class UnionAll extends PlanNode {
    private final List<PlanNode> inputs;

    public UnionAll(List<? extends PlanNode> inputs) {
      super(firstSchema(inputs));
      this.inputs = immutableCopy(inputs, "union input is null");
      for (int index = 1; index < this.inputs.size(); index++) {
        StructType schema = this.inputs.get(index).outputSchema();
        if (!outputSchema().equals(schema)) {
          throw new IllegalArgumentException(
              "UnionAll input " + index + " schema differs from input 0: " + schema);
        }
      }
    }

    public List<PlanNode> inputs() {
      return inputs;
    }

    private static StructType firstSchema(List<? extends PlanNode> inputs) {
      requireNonNull(inputs, "inputs is null");
      if (inputs.isEmpty()) {
        throw new IllegalArgumentException("UnionAll requires at least one input");
      }
      return requireNonNull(inputs.get(0), "union input is null").outputSchema();
    }
  }

  /** Groups input rows and computes aggregate columns. */
  public static final class Aggregate extends PlanNode {
    private final PlanNode input;
    private final List<Expression> groupBy;
    private final List<Agg> aggregates;

    public Aggregate(
        PlanNode input, List<Expression> groupBy, List<Agg> aggregates, StructType outputSchema) {
      super(outputSchema);
      requireNonNull(input, "input is null");
      this.input = input;
      this.groupBy = immutableCopy(groupBy, "grouping expression is null");
      this.aggregates = immutableCopy(aggregates, "aggregate is null");

      int expectedFields = this.groupBy.size() + this.aggregates.size();
      if (outputSchema().length() != expectedFields) {
        throw new IllegalArgumentException(
            "Aggregate output has "
                + outputSchema().length()
                + " fields, expected "
                + expectedFields);
      }
      for (int index = 0; index < this.groupBy.size(); index++) {
        RowKernels.requireHashable(outputSchema().at(index).getDataType());
      }
      for (int index = 0; index < this.aggregates.size(); index++) {
        Agg aggregate = this.aggregates.get(index);
        StructField field = outputSchema().at(this.groupBy.size() + index);
        if (!aggregate.resultType().equals(field.getDataType())
            || aggregate.resultNullable() != field.isNullable()) {
          throw new IllegalArgumentException(
              "Aggregate output field `"
                  + field.getName()
                  + "` must have type "
                  + aggregate.resultType()
                  + " and nullable="
                  + aggregate.resultNullable());
        }
      }
    }

    public PlanNode input() {
      return input;
    }

    public List<Expression> groupBy() {
      return groupBy;
    }

    public List<Agg> aggregates() {
      return aggregates;
    }
  }

  /** Filters probe rows by null-safe membership in the build keys. */
  public static final class SemiJoin extends PlanNode {
    private final PlanNode probe;
    private final PlanNode build;
    private final List<Expression> probeKeys;
    private final List<Expression> buildKeys;
    private final List<DataType> keyTypes;
    private final boolean inverted;

    public SemiJoin(
        PlanNode probe,
        PlanNode build,
        List<Expression> probeKeys,
        List<Expression> buildKeys,
        List<DataType> keyTypes,
        boolean inverted) {
      super(requireNonNull(probe, "probe is null").outputSchema());
      requireNonNull(build, "build is null");
      this.probe = probe;
      this.build = build;
      this.probeKeys = immutableCopy(probeKeys, "probe key is null");
      this.buildKeys = immutableCopy(buildKeys, "build key is null");
      this.keyTypes = immutableCopy(keyTypes, "key type is null");
      this.inverted = inverted;

      if (this.probeKeys.size() != this.buildKeys.size()
          || this.probeKeys.size() != this.keyTypes.size()) {
        throw new IllegalArgumentException(
            "SemiJoin probe keys, build keys, and key types must have equal arity");
      }
      for (int index = 0; index < this.keyTypes.size(); index++) {
        RowKernels.requireHashable(this.keyTypes.get(index));
      }
    }

    public PlanNode probe() {
      return probe;
    }

    public PlanNode build() {
      return build;
    }

    public List<Expression> probeKeys() {
      return probeKeys;
    }

    public List<Expression> buildKeys() {
      return buildKeys;
    }

    public List<DataType> keyTypes() {
      return keyTypes;
    }

    public boolean inverted() {
      return inverted;
    }
  }

  /** One execution-local reusable result. Every reference to an ID must name the same input. */
  public static final class Cte extends PlanNode {
    private final long id;
    private final PlanNode input;

    public Cte(long id, PlanNode input) {
      super(requireNonNull(input, "input is null").outputSchema());
      this.id = id;
      this.input = input;
    }

    public long id() {
      return id;
    }

    public PlanNode input() {
      return input;
    }
  }

  /** Inline owned rows matching a declared schema. */
  public static final class Values extends PlanNode {
    private final List<Row> ownedRows;

    public Values(StructType schema, List<? extends Row> ownedRows) {
      super(schema);
      this.ownedRows = immutableCopy(ownedRows, "row is null");
      for (int index = 0; index < this.ownedRows.size(); index++) {
        requireRow(this.ownedRows.get(index), schema, "Values row " + index);
      }
    }

    public List<Row> ownedRows() {
      return ownedRows;
    }
  }

  /** File format handled by a static or dynamic scan. */
  public enum Format {
    JSON,
    PARQUET
  }

  /** Reads a fixed set of files through the host engine. */
  public static final class FileScan extends PlanNode {
    private final Format format;
    private final List<ScanFile> files;
    private final Optional<URI> tableRoot;
    private final List<String> fileConstantColumns;
    private final Optional<Predicate> pushdownHint;
    private volatile boolean hashCodeMemoized;
    private int memoizedHashCode;

    public FileScan(
        Format format,
        List<ScanFile> files,
        Optional<URI> tableRoot,
        List<String> fileConstantColumns,
        StructType outputSchema,
        Optional<Predicate> pushdownHint) {
      super(outputSchema);
      this.format = requireNonNull(format, "format is null");
      this.files = immutableCopy(files, "scan file is null");
      this.tableRoot = requireNonNull(tableRoot, "tableRoot is null");
      this.fileConstantColumns = validateConstants(fileConstantColumns, outputSchema);
      this.pushdownHint = requireNonNull(pushdownHint, "pushdownHint is null");
      validateScan();
    }

    public static FileScan json(
        List<ScanFile> files,
        Optional<URI> tableRoot,
        List<String> fileConstantColumns,
        StructType outputSchema) {
      return new FileScan(
          Format.JSON, files, tableRoot, fileConstantColumns, outputSchema, Optional.empty());
    }

    public static FileScan parquet(
        List<ScanFile> files,
        Optional<URI> tableRoot,
        List<String> fileConstantColumns,
        StructType outputSchema,
        Optional<Predicate> pushdownHint) {
      return new FileScan(
          Format.PARQUET, files, tableRoot, fileConstantColumns, outputSchema, pushdownHint);
    }

    public Format format() {
      return format;
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

    public Optional<Predicate> pushdownHint() {
      return pushdownHint;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof FileScan)) {
        return false;
      }
      FileScan that = (FileScan) other;
      return outputSchema().equals(that.outputSchema())
          && format == that.format
          && files.equals(that.files)
          && tableRoot.equals(that.tableRoot)
          && fileConstantColumns.equals(that.fileConstantColumns)
          && pushdownHint.equals(that.pushdownHint);
    }

    @Override
    public int hashCode() {
      if (!hashCodeMemoized) {
        memoizedHashCode =
            Objects.hash(
                outputSchema(), format, files, tableRoot, fileConstantColumns, pushdownHint);
        hashCodeMemoized = true;
      }
      return memoizedHashCode;
    }

    private void validateScan() {
      if (tableRoot.isPresent() && !tableRoot.get().isAbsolute()) {
        throw new IllegalArgumentException("Table root is not an absolute URI: " + tableRoot.get());
      }
      if (format == Format.JSON && pushdownHint.isPresent()) {
        throw new IllegalArgumentException("JSON scan cannot have a pushdown hint");
      }
      StructType constantsSchema = constantsSchema(outputSchema(), fileConstantColumns);
      for (StructField field : constantsSchema.fields()) {
        RowKernels.requireHashable(field.getDataType());
      }
      Set<String> paths = new HashSet<>();
      for (int index = 0; index < files.size(); index++) {
        ScanFile file = files.get(index);
        String path = file.getFileStatus().getPath();
        if (!paths.add(path)) {
          throw new IllegalArgumentException("Duplicate scan file path `" + path + "`");
        }
        requireRow(file.getFileConstants(), constantsSchema, "Scan file " + index + " constants");
        Optional<DeletionVectorDescriptor> dv = file.getDeletionVector();
        if (dv.isPresent()
            && DeletionVectorDescriptor.UUID_DV_MARKER.equals(dv.get().getStorageType())
            && !tableRoot.isPresent()) {
          throw new IllegalArgumentException(
              "Table root is required for relative deletion vectors");
        }
      }
    }

    private static List<String> validateConstants(List<String> names, StructType outputSchema) {
      List<String> copy = immutableCopy(names, "file constant column is null");
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
      java.util.ArrayList<StructField> fields = new java.util.ArrayList<>(names.size());
      for (String name : names) {
        fields.add(schema.get(name));
      }
      return new StructType(fields);
    }
  }

  private static void requireRow(Row row, StructType schema, String context) {
    requireNonNull(row, "row is null");
    if (!schema.equals(requireNonNull(row.getSchema(), "row schema is null"))) {
      throw new IllegalArgumentException(context + " schema differs from " + schema);
    }
    for (int ordinal = 0; ordinal < schema.length(); ordinal++) {
      StructField field = schema.at(ordinal);
      if (!field.isNullable() && row.isNullAt(ordinal)) {
        throw new IllegalArgumentException(
            context + " has null for non-nullable field `" + field.getName() + "`");
      }
    }
  }

  private static <T> List<T> immutableCopy(List<? extends T> values, String nullMessage) {
    requireNonNull(values, "values is null");
    java.util.ArrayList<T> copy = new java.util.ArrayList<>(values.size());
    for (T value : values) {
      copy.add(requireNonNull(value, nullMessage));
    }
    return Collections.unmodifiableList(copy);
  }
}
