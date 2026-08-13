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

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.internal.plans.Filter;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

/** Executes a {@link Filter} by lazily adding its predicate to each input batch's selection. */
final class FilterExecutor {
  private FilterExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(
      Filter filter, StructType inputSchema, CloseableIterator<FilteredColumnarBatch> input) {
    requireNonNull(filter, "filter is null");
    requireNonNull(inputSchema, "inputSchema is null");
    requireNonNull(input, "input is null");

    ExpressionEvaluator evaluator =
        new DefaultExpressionEvaluator(inputSchema, filter.getPredicate(), BooleanType.BOOLEAN);
    return input.map(batch -> evaluateBatch(inputSchema, evaluator, batch));
  }

  private static FilteredColumnarBatch evaluateBatch(
      StructType inputSchema, ExpressionEvaluator evaluator, FilteredColumnarBatch batch) {
    requireNonNull(batch, "input batch is null");
    ColumnarBatch data = batch.getData();
    checkArgument(
        inputSchema.equals(data.getSchema()),
        "Filter input batch schema %s does not match expected schema %s",
        data.getSchema(),
        inputSchema);
    return batch.withSelectionVector(evaluator.eval(data));
  }
}
