/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.kernel.expressions;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.FilteredColumnarBatch;

/**
 * Interface for implementing an {@link Expression} evaluator. It contains one {@link Expression}
 * which can be evaluated on multiple batches. Connectors can implement this interface to optimize
 * the evaluation using connector-specific capabilities.
 *
 * @since 3.0.0
 */
@Evolving
public interface ExpressionEvaluator extends AutoCloseable {
  /**
   * Evaluate the expression on the given batch.
   *
   * <p>The output has the same row count, row order, and selection as {@code input}. Its top-level
   * columns are the fields of the evaluator's declared output schema. The result declares whether
   * it borrows input or evaluator state through {@link FilteredColumnarBatch#getLifetime()}. The
   * lifetime is declared independently for each result batch.
   *
   * @param input input data and selection
   * @return the evaluated output batch
   */
  FilteredColumnarBatch eval(FilteredColumnarBatch input);
}
