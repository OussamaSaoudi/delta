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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.internal.plans.Filter;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

/** Adds a lazy predicate selection to each input batch without copying its data. */
final class FilterExecutor {
  private FilterExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(
      Filter filter, StructType inputSchema, CloseableIterator<FilteredColumnarBatch> input) {
    requireNonNull(filter, "filter is null");
    requireNonNull(input, "input is null");
    ExpressionEvaluator evaluator =
        new DefaultExpressionEvaluator(
            requireNonNull(inputSchema, "inputSchema is null"),
            filter.getPredicate(),
            BooleanType.BOOLEAN);
    return input.map(batch -> evaluate(inputSchema, evaluator, batch));
  }

  private static FilteredColumnarBatch evaluate(
      StructType inputSchema, ExpressionEvaluator evaluator, FilteredColumnarBatch batch) {
    requireNonNull(batch, "input batch is null");
    ColumnarBatch data = batch.getData();
    if (!inputSchema.equals(data.getSchema())) {
      throw new IllegalArgumentException(
          "Filter input batch schema "
              + data.getSchema()
              + " does not match expected schema "
              + inputSchema);
    }
    return batch.withSelectionVector(evaluator.eval(data));
  }
}
