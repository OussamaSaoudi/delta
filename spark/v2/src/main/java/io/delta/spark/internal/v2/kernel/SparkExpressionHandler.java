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
package io.delta.spark.internal.v2.kernel;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.expressions.StructExpression;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.utils.KernelRowToSparkRow;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.spark.sql.catalyst.CatalystTypeConverters$;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.InterpretedPredicate;
import scala.Function1;

/** Evaluates Spark opaque expressions and delegates ordinary Kernel expressions. */
public final class SparkExpressionHandler implements ExpressionHandler {
  private final ExpressionHandler delegate;

  public SparkExpressionHandler(ExpressionHandler delegate) {
    this.delegate = requireNonNull(delegate, "delegate is null");
  }

  @Override
  public ExpressionEvaluator getEvaluator(
      StructType inputSchema, Expression expression, StructType outputSchema) {
    requireNonNull(inputSchema, "inputSchema is null");
    requireNonNull(expression, "expression is null");
    requireNonNull(outputSchema, "outputSchema is null");
    if (expression instanceof SparkOpaqueExpression) {
      return new SparkEvaluator(inputSchema, (SparkOpaqueExpression) expression, outputSchema);
    }
    if (isSparkStruct(expression)) {
      return new SparkEvaluator(inputSchema, (StructExpression) expression, outputSchema);
    }
    return delegate.getEvaluator(inputSchema, expression, outputSchema);
  }

  @Override
  public PredicateEvaluator getPredicateEvaluator(StructType inputSchema, Predicate predicate) {
    requireNonNull(inputSchema, "inputSchema is null");
    requireNonNull(predicate, "predicate is null");
    if (!(predicate instanceof SparkOpaquePredicate)) {
      return delegate.getPredicateEvaluator(inputSchema, predicate);
    }
    return new SparkPredicateEvaluator(inputSchema, (SparkOpaquePredicate) predicate, delegate);
  }

  @Override
  public ColumnVector createSelectionVector(boolean[] values, int from, int to) {
    return delegate.createSelectionVector(values, from, to);
  }

  private static boolean isSparkStruct(Expression expression) {
    if (!(expression instanceof StructExpression)) {
      return false;
    }
    StructExpression struct = (StructExpression) expression;
    if (struct.getNullabilityPredicate().isPresent()) {
      return false;
    }
    return !struct.getFieldExpressions().isEmpty()
        && struct.getFieldExpressions().stream().allMatch(SparkOpaqueExpression.class::isInstance);
  }

  private static void validateBound(
      org.apache.spark.sql.catalyst.expressions.Expression expression) {
    if (!expression.references().isEmpty()) {
      throw new IllegalArgumentException(
          "Catalyst expression must be bound to input ordinals: " + expression);
    }
  }

  private static IntFunction<InternalRow> inputRows(
      FilteredColumnarBatch input, org.apache.spark.sql.types.StructType sparkSchema) {
    if (input.getData() instanceof SparkRowArrayBatch) {
      SparkRowArrayBatch rows = (SparkRowArrayBatch) input.getData();
      return rows::backingRow;
    }

    InternalRow[] rows = new InternalRow[input.getData().getSize()];
    Function1<Object, Object> converter =
        CatalystTypeConverters$.MODULE$.createToCatalystConverter(sparkSchema);
    CloseableIterator<Row> inputRows = input.getData().getRows();
    try {
      int rowId = 0;
      while (inputRows.hasNext()) {
        if (rowId == rows.length) {
          throw new IllegalStateException("Kernel batch returned too many rows");
        }
        rows[rowId++] =
            (InternalRow) converter.apply(new KernelRowToSparkRow(inputRows.next(), sparkSchema));
      }
      if (rowId != rows.length) {
        throw new IllegalStateException("Kernel batch returned too few rows");
      }
    } catch (RuntimeException | Error failure) {
      Utils.closeCloseablesAndAddSuppressed(failure, inputRows);
      throw failure;
    }
    Utils.closeCloseables(inputRows);
    return rowId -> rows[rowId];
  }

  private static final class SparkEvaluator implements ExpressionEvaluator {
    private final org.apache.spark.sql.types.StructType sparkInputSchema;
    private final org.apache.spark.sql.types.StructType sparkOutputSchema;
    private final StructType outputSchema;
    private final org.apache.spark.sql.catalyst.expressions.Expression rowExpression;
    private final List<org.apache.spark.sql.catalyst.expressions.Expression> fieldExpressions;

    private SparkEvaluator(
        StructType inputSchema, SparkOpaqueExpression expression, StructType outputSchema) {
      this.sparkInputSchema = SchemaUtils.convertKernelSchemaToSparkSchema(inputSchema);
      this.sparkOutputSchema = SchemaUtils.convertKernelSchemaToSparkSchema(outputSchema);
      this.outputSchema = outputSchema;
      this.rowExpression = expression.catalyst();
      this.fieldExpressions = null;
      validateBound(rowExpression);
      if (!sparkOutputSchema.equals(rowExpression.dataType())) {
        throw new IllegalArgumentException(
            "Catalyst expression type does not match output schema: "
                + rowExpression.dataType()
                + " != "
                + sparkOutputSchema);
      }
    }

    private SparkEvaluator(
        StructType inputSchema, StructExpression expression, StructType outputSchema) {
      this.sparkInputSchema = SchemaUtils.convertKernelSchemaToSparkSchema(inputSchema);
      this.sparkOutputSchema = SchemaUtils.convertKernelSchemaToSparkSchema(outputSchema);
      this.outputSchema = outputSchema;
      this.rowExpression = null;
      this.fieldExpressions = new ArrayList<>(expression.getFieldExpressions().size());
      if (expression.getFieldExpressions().size() != outputSchema.length()) {
        throw new IllegalArgumentException("Struct expression does not match output field count");
      }
      for (int ordinal = 0; ordinal < outputSchema.length(); ordinal++) {
        org.apache.spark.sql.catalyst.expressions.Expression field =
            ((SparkOpaqueExpression) expression.getFieldExpressions().get(ordinal)).catalyst();
        validateBound(field);
        if (!sparkOutputSchema.fields()[ordinal].dataType().equals(field.dataType())) {
          throw new IllegalArgumentException(
              "Catalyst field type does not match output field " + ordinal);
        }
        fieldExpressions.add(field);
      }
    }

    @Override
    public FilteredColumnarBatch eval(FilteredColumnarBatch input) {
      requireNonNull(input, "input is null");
      IntFunction<InternalRow> inputRows = inputRows(input, sparkInputSchema);
      InternalRow[] outputRows = new InternalRow[input.getData().getSize()];
      for (int rowId = 0; rowId < outputRows.length; rowId++) {
        InternalRow inputRow = inputRows.apply(rowId);
        if (rowExpression != null) {
          Object value = rowExpression.eval(inputRow);
          if (!(value instanceof InternalRow)) {
            throw new IllegalStateException("Catalyst row expression returned " + value);
          }
          outputRows[rowId] = (InternalRow) value;
        } else {
          Object[] values = new Object[fieldExpressions.size()];
          for (int ordinal = 0; ordinal < values.length; ordinal++) {
            values[ordinal] = fieldExpressions.get(ordinal).eval(inputRow);
          }
          outputRows[rowId] = new GenericInternalRow(values);
        }
      }
      SparkRowArrayBatch output =
          new SparkRowArrayBatch(outputSchema, sparkOutputSchema, outputRows, outputRows.length);
      return input.withData(output, FilteredColumnarBatch.Lifetime.BORROWED);
    }

    @Override
    public void close() {}
  }

  private static final class SparkPredicateEvaluator implements PredicateEvaluator {
    private final org.apache.spark.sql.types.StructType sparkInputSchema;
    private final InterpretedPredicate predicate;
    private final ExpressionHandler expressions;

    private SparkPredicateEvaluator(
        StructType inputSchema, SparkOpaquePredicate predicate, ExpressionHandler expressions) {
      this.sparkInputSchema = SchemaUtils.convertKernelSchemaToSparkSchema(inputSchema);
      validateBound(predicate.catalyst());
      this.predicate =
          org.apache.spark.sql.catalyst.expressions.Predicate.createInterpreted(
              predicate.catalyst());
      this.predicate.initialize(0);
      this.expressions = expressions;
    }

    @Override
    public FilteredColumnarBatch eval(FilteredColumnarBatch input) {
      requireNonNull(input, "input is null");
      IntFunction<InternalRow> inputRows = inputRows(input, sparkInputSchema);
      boolean[] selected = new boolean[input.getData().getSize()];
      for (int rowId = 0; rowId < selected.length; rowId++) {
        selected[rowId] = predicate.eval(inputRows.apply(rowId));
      }
      ColumnVector selection = expressions.createSelectionVector(selected, 0, selected.length);
      return input.withSelectionVector(selection, input.getLifetime());
    }
  }
}
