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
package io.delta.kernel.defaults.internal.plans;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultViewVector;
import io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.StructExpression;
import io.delta.kernel.expressions.StructPatch;
import io.delta.kernel.internal.plans.Project;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Collections;

/** Executes a {@link Project} lazily over Kernel Java batches. */
final class ProjectExecutor {
  private ProjectExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(
      Project project, StructType inputSchema, CloseableIterator<FilteredColumnarBatch> input) {
    requireNonNull(project, "project is null");
    requireNonNull(inputSchema, "inputSchema is null");
    requireNonNull(input, "input is null");

    StructType outputSchema = project.getOutputSchema(Collections.singletonList(inputSchema));
    ExpressionEvaluator evaluator =
        new DefaultExpressionEvaluator(inputSchema, project.getExpression(), outputSchema);
    boolean nullableRoot =
        project.getExpression() instanceof StructExpression
            ? ((StructExpression) project.getExpression()).getNullabilityPredicate().isPresent()
            : ((StructPatch) project.getExpression()).getInputPath().isPresent();
    return input.map(
        batch -> evaluateBatch(inputSchema, outputSchema, nullableRoot, evaluator, batch));
  }

  private static FilteredColumnarBatch evaluateBatch(
      StructType inputSchema,
      StructType outputSchema,
      boolean nullableRoot,
      ExpressionEvaluator evaluator,
      FilteredColumnarBatch batch) {
    requireNonNull(batch, "input batch is null");
    ColumnarBatch data = batch.getData();
    checkArgument(
        inputSchema.equals(data.getSchema()),
        "Project input batch schema %s does not match expected schema %s",
        data.getSchema(),
        inputSchema);

    ColumnVector root = requireNonNull(evaluator.eval(data), "Project evaluator returned null");
    try {
      checkArgument(
          outputSchema.equals(root.getDataType()),
          "Project evaluator returned type %s, expected %s",
          root.getDataType(),
          outputSchema);
      checkArgument(
          root.getSize() == data.getSize(),
          "Project evaluator returned %s rows, expected %s",
          root.getSize(),
          data.getSize());
      return batch.withData(resultBatch(outputSchema, root, nullableRoot));
    } catch (RuntimeException failure) {
      try {
        root.close();
      } catch (RuntimeException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }

  private static ColumnarBatch resultBatch(
      StructType schema, ColumnVector root, boolean nullableRoot) {
    ColumnVector projectedRoot =
        nullableRoot ? new DefaultViewVector(root, 0, root.getSize()) : root;
    ColumnVector[] columns = new ColumnVector[schema.length()];
    for (int ordinal = 0; ordinal < columns.length; ordinal++) {
      columns[ordinal] = projectedRoot.getChild(ordinal);
    }
    return new DefaultColumnarBatch(root.getSize(), schema, columns);
  }
}
