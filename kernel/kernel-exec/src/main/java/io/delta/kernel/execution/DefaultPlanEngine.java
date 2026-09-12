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
package io.delta.kernel.execution;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.expressions.StructExpression;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.data.RowBackedColumnarBatch;
import io.delta.kernel.internal.deletionvectors.DeletionVectorStoredBitmap;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.util.RowKernels;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.plans.PlanNode.FileScan;
import io.delta.kernel.plans.PlanNode.Format;
import io.delta.kernel.plans.ScanFile;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.GeographyType;
import io.delta.kernel.types.GeometryType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.MetadataColumnSpec;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Adapts the existing Kernel {@link Engine} API to shared plan execution. */
final class DefaultPlanEngine implements PlanEngine {
  private static final String PRIVATE_ROW_INDEX = "__delta_kernel_scan_row_index";

  private final Engine engine;
  private final ExpressionHandler expressions;

  DefaultPlanEngine(Engine engine) {
    this.engine = requireNonNull(engine, "engine is null");
    this.expressions = requireNonNull(engine.getExpressionHandler(), "expression handler is null");
  }

  @Override
  public BatchEvaluator bind(
      StructType inputSchema, Expression expression, StructType outputSchema) {
    if (expression instanceof StructExpression) {
      return bindFields(inputSchema, ((StructExpression) expression).fields(), outputSchema);
    }
    ExpressionEvaluator evaluator = expressions.getEvaluator(inputSchema, expression, outputSchema);
    return new BatchEvaluator() {
      private ColumnVector result;
      private boolean closed;

      @Override
      public ColumnarBatch eval(ColumnarBatch input) {
        if (closed) {
          throw new IllegalStateException("Evaluator is closed");
        }
        Utils.closeCloseables(result);
        result = evaluator.eval(input);
        ColumnVector[] columns = new ColumnVector[outputSchema.length()];
        for (int ordinal = 0; ordinal < columns.length; ordinal++) {
          columns[ordinal] = result.getChild(ordinal);
        }
        return new VectorBatch(
            outputSchema, input.getSize(), columns, ColumnarBatch.Lifetime.BORROWED);
      }

      @Override
      public void close() {
        if (!closed) {
          closed = true;
          Utils.closeCloseables(result, evaluator);
          result = null;
        }
      }
    };
  }

  private BatchEvaluator bindFields(
      StructType inputSchema, List<Expression> expressions, StructType outputSchema) {
    if (expressions.size() != outputSchema.length()) {
      throw new IllegalArgumentException("Expression count does not match output schema");
    }
    ExpressionEvaluator[] evaluators = new ExpressionEvaluator[expressions.size()];
    for (int ordinal = 0; ordinal < evaluators.length; ordinal++) {
      evaluators[ordinal] =
          this.expressions.getEvaluator(
              inputSchema, expressions.get(ordinal), outputSchema.at(ordinal).getDataType());
    }
    return new BatchEvaluator() {
      private ColumnVector[] results = new ColumnVector[0];
      private boolean closed;

      @Override
      public ColumnarBatch eval(ColumnarBatch input) {
        if (closed) {
          throw new IllegalStateException("Evaluator is closed");
        }
        Utils.closeCloseables(results);
        results = new ColumnVector[evaluators.length];
        for (int ordinal = 0; ordinal < results.length; ordinal++) {
          results[ordinal] = evaluators[ordinal].eval(input);
        }
        return new VectorBatch(
            outputSchema, input.getSize(), results, ColumnarBatch.Lifetime.BORROWED);
      }

      @Override
      public void close() {
        if (!closed) {
          closed = true;
          Utils.closeCloseables(results);
          results = new ColumnVector[0];
          Utils.closeCloseables(evaluators);
        }
      }
    };
  }

  @Override
  public BatchEvaluator bind(StructType inputSchema, Predicate predicate) {
    PredicateEvaluator evaluator = expressions.getPredicateEvaluator(inputSchema, predicate);
    return input -> {
      ColumnVector selection = evaluator.eval(input, Optional.empty());
      try {
        boolean[] keep = new boolean[input.getSize()];
        for (int rowId = 0; rowId < keep.length; rowId++) {
          keep[rowId] = !selection.isNullAt(rowId) && selection.getBoolean(rowId);
        }
        return filter(input, keep);
      } finally {
        selection.close();
      }
    };
  }

  @Override
  public ColumnarBatch filter(ColumnarBatch input, boolean[] keep) {
    requireNonNull(input, "input is null");
    if (keep == null || keep.length != input.getSize()) {
      throw new IllegalArgumentException("Filter length must match batch size");
    }
    if (input instanceof RowBackedColumnarBatch) {
      return ((RowBackedColumnarBatch) input).filter(keep);
    }
    List<Row> rows = new ArrayList<>();
    for (int rowId = 0; rowId < keep.length; rowId++) {
      if (keep[rowId]) {
        rows.add(RowKernels.rowAt(input, rowId));
      }
    }
    return new RowBackedColumnarBatch(input.getSchema(), rows, input.getLifetime());
  }

  @Override
  public Row retainRow(Row input, StructType schema) {
    requireNonNull(input, "input is null");
    requireNonNull(schema, "schema is null");
    if (input.getSchema().length() < schema.length()) {
      throw new IllegalArgumentException("Retained row is wider than its input");
    }
    Map<Integer, Object> values = new HashMap<>();
    for (int ordinal = 0; ordinal < schema.length(); ordinal++) {
      DataType type = schema.at(ordinal).getDataType();
      if (!type.equals(input.getSchema().at(ordinal).getDataType())) {
        throw new IllegalArgumentException("Retained row field type does not match input");
      }
      Object value = materializeValue(input, ordinal, type);
      if (value != null) {
        values.put(ordinal, value);
      }
    }
    return new GenericRow(schema, values);
  }

  @Override
  public Row retainValue(Row input, int ordinal, StructType outputSchema) {
    requireNonNull(input, "input is null");
    requireNonNull(outputSchema, "outputSchema is null");
    if (outputSchema.length() != 1
        || !outputSchema.at(0).getDataType().equals(input.getSchema().at(ordinal).getDataType())) {
      throw new IllegalArgumentException("Retained value schema does not match input");
    }
    Object value = materializeValue(input, ordinal, outputSchema.at(0).getDataType());
    return value == null ? null : new GenericRow(outputSchema, Collections.singletonMap(0, value));
  }

  @Override
  public Row longValue(long value, StructType outputSchema) {
    requireNonNull(outputSchema, "outputSchema is null");
    if (outputSchema.length() != 1 || !(outputSchema.at(0).getDataType() instanceof LongType)) {
      throw new IllegalArgumentException("LONG value requires a one-field LONG schema");
    }
    return new GenericRow(outputSchema, Collections.singletonMap(0, value));
  }

  @Override
  public ColumnarBatch appendColumns(
      ColumnarBatch input, StructType outputSchema, List<List<Row>> columns) {
    requireNonNull(input, "input is null");
    requireNonNull(columns, "columns is null");
    int inputWidth = input.getSchema().length();
    if (outputSchema.length() != inputWidth + columns.size()) {
      throw new IllegalArgumentException("Output schema does not match appended columns");
    }
    ColumnVector[] result = new ColumnVector[outputSchema.length()];
    for (int ordinal = 0; ordinal < inputWidth; ordinal++) {
      result[ordinal] = input.getColumnVector(ordinal);
    }
    for (int ordinal = 0; ordinal < columns.size(); ordinal++) {
      StructField field = outputSchema.at(inputWidth + ordinal);
      StructType valueSchema = new StructType().add(field("_value", field.getDataType()));
      result[inputWidth + ordinal] =
          new RowBackedColumnarBatch(
                  valueSchema, columns.get(ordinal), ColumnarBatch.Lifetime.OWNED)
              .getColumnVector(0);
    }
    return new VectorBatch(outputSchema, input.getSize(), result, input.getLifetime());
  }

  @Override
  public CloseableIterator<ColumnarBatch> scan(FileScan scan) {
    requireNonNull(scan, "scan is null");
    BoundScan bound = new BoundScan(scan);
    return scan.format() == Format.JSON ? openJson(bound) : openParquet(bound);
  }

  private CloseableIterator<ColumnarBatch> openJson(BoundScan scan) {
    return Utils.toCloseableIterator(scan.node.files().iterator())
        .flatMap(
            file -> {
              CloseableIterator<FileStatus> status =
                  Utils.singletonCloseableIterator(file.getFileStatus());
              CloseableIterator<ColumnarBatch> reader = openJsonReader(status, scan.readSchema);
              RoaringBitmapArray bitmap = scan.loadDeletionVector(file);
              long[] nextRowIndex = {0};
              return reader.map(
                  batch -> {
                    long firstRowIndex = nextRowIndex[0];
                    nextRowIndex[0] = Math.addExact(firstRowIndex, batch.getSize());
                    return scan.finish(file, batch, firstRowIndex, bitmap);
                  });
            });
  }

  private CloseableIterator<ColumnarBatch> openParquet(BoundScan scan) {
    Map<String, ScanFile> files = new HashMap<>();
    for (ScanFile file : scan.node.files()) {
      files.put(file.getFileStatus().getPath(), file);
    }
    Map<String, RoaringBitmapArray> bitmaps = new HashMap<>();
    CloseableIterator<FileStatus> statuses =
        Utils.toCloseableIterator(
            scan.node.files().stream().map(ScanFile::getFileStatus).iterator());
    return openParquetReader(statuses, scan.readSchema, scan.node.pushdownHint())
        .map(
            result -> {
              ScanFile file = files.get(result.getFilePath());
              if (file == null) {
                throw new IllegalArgumentException(
                    "Parquet reader returned unknown file " + result.getFilePath());
              }
              RoaringBitmapArray bitmap =
                  file.getDeletionVector().isPresent()
                      ? bitmaps.computeIfAbsent(
                          result.getFilePath(), ignored -> scan.loadDeletionVector(file))
                      : null;
              return scan.finish(file, result.getData(), 0, bitmap);
            });
  }

  private CloseableIterator<ColumnarBatch> openJsonReader(
      CloseableIterator<FileStatus> statuses, StructType readSchema) {
    try {
      return requireNonNull(
          engine.getJsonHandler().readJsonFiles(statuses, readSchema, Optional.empty()),
          "JSON reader is null");
    } catch (IOException failure) {
      Utils.closeCloseablesSilently(statuses);
      throw new UncheckedIOException("Failed to open JSON scan", failure);
    }
  }

  private CloseableIterator<FileReadResult> openParquetReader(
      CloseableIterator<FileStatus> statuses,
      StructType readSchema,
      Optional<Predicate> pushdownHint) {
    try {
      return requireNonNull(
          engine.getParquetHandler().readParquetFiles(statuses, readSchema, pushdownHint),
          "Parquet reader is null");
    } catch (IOException failure) {
      Utils.closeCloseablesSilently(statuses);
      throw new UncheckedIOException("Failed to open Parquet scan", failure);
    }
  }

  /** Schemas and column positions bound once for one scan. */
  private final class BoundScan {
    private final FileScan node;
    private final boolean json;
    private final boolean dropRowIndex;
    private final StructType outputSchema;
    private final StructType readSchema;
    private final StructType constantsSchema;
    private final int rowIndexOrdinal;
    private final int[] readOrdinals;
    private final int[] constantOrdinals;

    private BoundScan(FileScan node) {
      this.node = node;
      this.json = node.format() == Format.JSON;
      this.dropRowIndex = requiresPrivateRowIndex(node);
      this.outputSchema =
          dropRowIndex
              ? node.outputSchema()
                  .add(
                      StructField.createMetadataColumn(
                          PRIVATE_ROW_INDEX, MetadataColumnSpec.ROW_INDEX))
              : node.outputSchema();
      this.rowIndexOrdinal = outputSchema.indexOf(MetadataColumnSpec.ROW_INDEX);
      this.readOrdinals = new int[outputSchema.length()];
      this.constantOrdinals = new int[outputSchema.length()];

      List<StructField> readFields = new ArrayList<>();
      List<StructField> constantFields = new ArrayList<>();
      for (String name : node.fileConstantColumns()) {
        constantFields.add(outputSchema.get(name));
      }
      for (int outputOrdinal = 0; outputOrdinal < outputSchema.length(); outputOrdinal++) {
        StructField field = outputSchema.at(outputOrdinal);
        int constantOrdinal = node.fileConstantColumns().indexOf(field.getName());
        constantOrdinals[outputOrdinal] = constantOrdinal;
        readOrdinals[outputOrdinal] = -1;
        if (constantOrdinal >= 0) {
          continue;
        }
        if (json && field.isMetadataColumn()) {
          if (field.getMetadataColumnSpec() != MetadataColumnSpec.ROW_INDEX) {
            throw new IllegalArgumentException(
                "Unsupported JSON metadata column `" + field.getName() + "`");
          }
          continue;
        }
        readOrdinals[outputOrdinal] = readFields.size();
        readFields.add(field);
      }
      this.readSchema = new StructType(readFields);
      this.constantsSchema = new StructType(constantFields);
    }

    private ColumnarBatch finish(
        ScanFile file, ColumnarBatch input, long firstRowIndex, RoaringBitmapArray bitmap) {
      if (!readSchema.equals(input.getSchema())) {
        throw new IllegalArgumentException(
            "Scan reader returned schema " + input.getSchema() + ", expected " + readSchema);
      }
      ColumnarBatch constants =
          constantsSchema.length() == 0
              ? null
              : new RowBackedColumnarBatch(
                  constantsSchema,
                  Collections.nCopies(input.getSize(), file.getFileConstants()),
                  ColumnarBatch.Lifetime.OWNED);
      ColumnVector[] columns = new ColumnVector[outputSchema.length()];
      for (int ordinal = 0; ordinal < columns.length; ordinal++) {
        if (constantOrdinals[ordinal] >= 0) {
          columns[ordinal] = constants.getColumnVector(constantOrdinals[ordinal]);
        } else if (readOrdinals[ordinal] >= 0) {
          columns[ordinal] = input.getColumnVector(readOrdinals[ordinal]);
        } else {
          columns[ordinal] = new RowIndexVector(input.getSize(), firstRowIndex);
        }
      }
      ColumnarBatch output =
          new VectorBatch(outputSchema, input.getSize(), columns, input.getLifetime());
      if (bitmap != null) {
        ColumnVector rowIndices = output.getColumnVector(rowIndexOrdinal);
        boolean[] keep = new boolean[output.getSize()];
        for (int rowId = 0; rowId < keep.length; rowId++) {
          keep[rowId] = !bitmap.contains(rowIndices.getLong(rowId));
        }
        output = filter(output, keep);
      }
      return dropRowIndex ? dropColumn(output, rowIndexOrdinal) : output;
    }

    private RoaringBitmapArray loadDeletionVector(ScanFile file) {
      Optional<DeletionVectorDescriptor> descriptor = file.getDeletionVector();
      if (!descriptor.isPresent()) {
        return null;
      }
      try {
        return new DeletionVectorStoredBitmap(descriptor.get(), node.tableRoot().map(URI::toString))
            .load(engine.getFileSystemClient());
      } catch (IOException failure) {
        throw new UncheckedIOException(
            "Failed to load deletion vector for " + file.getFileStatus().getPath(), failure);
      }
    }

    private boolean requiresPrivateRowIndex(FileScan scan) {
      return scan.outputSchema().indexOf(MetadataColumnSpec.ROW_INDEX) < 0
          && scan.files().stream().anyMatch(file -> file.getDeletionVector().isPresent());
    }
  }

  private static ColumnarBatch dropColumn(ColumnarBatch input, int dropped) {
    if (dropped < 0) {
      throw new IllegalArgumentException("Column to drop is absent");
    }
    List<StructField> fields = new ArrayList<>(input.getSchema().fields());
    fields.remove(dropped);
    ColumnVector[] columns = new ColumnVector[input.getSchema().length() - 1];
    for (int source = 0, target = 0; source < input.getSchema().length(); source++) {
      if (source != dropped) {
        columns[target++] = input.getColumnVector(source);
      }
    }
    return new VectorBatch(new StructType(fields), input.getSize(), columns, input.getLifetime());
  }

  private static Object materializeValue(Row row, int ordinal, DataType type) {
    if (row.isNullAt(ordinal)) {
      return null;
    }
    if (type instanceof BooleanType) {
      return row.getBoolean(ordinal);
    }
    if (type instanceof ByteType) {
      return row.getByte(ordinal);
    }
    if (type instanceof ShortType) {
      return row.getShort(ordinal);
    }
    if (type instanceof IntegerType || type instanceof DateType) {
      return row.getInt(ordinal);
    }
    if (type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType) {
      return row.getLong(ordinal);
    }
    if (type instanceof FloatType) {
      return row.getFloat(ordinal);
    }
    if (type instanceof DoubleType) {
      return row.getDouble(ordinal);
    }
    if (type instanceof DecimalType) {
      return row.getDecimal(ordinal);
    }
    if (type instanceof StringType
        || type instanceof GeometryType
        || type instanceof GeographyType) {
      return row.getString(ordinal);
    }
    if (type instanceof BinaryType) {
      return row.getBinary(ordinal).clone();
    }
    if (type instanceof StructType) {
      Row value = row.getStruct(ordinal);
      Map<Integer, Object> values = new HashMap<>();
      StructType schema = (StructType) type;
      for (int child = 0; child < schema.length(); child++) {
        Object retained = materializeValue(value, child, schema.at(child).getDataType());
        if (retained != null) {
          values.put(child, retained);
        }
      }
      return new GenericRow(schema, values);
    }
    if (type instanceof ArrayType || type instanceof MapType) {
      throw new UnsupportedOperationException("Aggregate retention does not support " + type);
    }
    throw new UnsupportedOperationException("Unsupported retained value type " + type);
  }

  private static StructField field(String name, DataType type) {
    return new StructField(name, type, true);
  }

  /** Zero-copy batch view over engine-owned vectors. */
  private static final class VectorBatch implements ColumnarBatch {
    private final StructType schema;
    private final int size;
    private final ColumnVector[] columns;
    private final Lifetime lifetime;

    private VectorBatch(StructType schema, int size, ColumnVector[] columns, Lifetime lifetime) {
      this.schema = requireNonNull(schema, "schema is null");
      this.size = size;
      this.columns = requireNonNull(columns, "columns is null").clone();
      this.lifetime = requireNonNull(lifetime, "lifetime is null");
      if (size < 0 || schema.length() != columns.length) {
        throw new IllegalArgumentException("Invalid batch shape");
      }
      for (int ordinal = 0; ordinal < columns.length; ordinal++) {
        ColumnVector column = requireNonNull(columns[ordinal], "column is null");
        if (column.getSize() != size
            || !schema.at(ordinal).getDataType().equals(column.getDataType())) {
          throw new IllegalArgumentException("Column does not match batch schema");
        }
      }
    }

    @Override
    public StructType getSchema() {
      return schema;
    }

    @Override
    public ColumnVector getColumnVector(int ordinal) {
      return columns[ordinal];
    }

    @Override
    public int getSize() {
      return size;
    }

    @Override
    public Lifetime getLifetime() {
      return lifetime;
    }
  }

  /** Synthetic JSON row index without allocating one boxed value per row. */
  private static final class RowIndexVector implements ColumnVector {
    private final int size;
    private final long first;

    private RowIndexVector(int size, long first) {
      this.size = size;
      this.first = first;
    }

    @Override
    public DataType getDataType() {
      return LongType.LONG;
    }

    @Override
    public int getSize() {
      return size;
    }

    @Override
    public void close() {}

    @Override
    public boolean isNullAt(int rowId) {
      return false;
    }

    @Override
    public long getLong(int rowId) {
      if (rowId < 0 || rowId >= size) {
        throw new IndexOutOfBoundsException("Invalid row id: " + rowId);
      }
      return Math.addExact(first, rowId);
    }
  }
}
