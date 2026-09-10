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
 * Evaluates a {@link Predicate} by narrowing the selection of a batch without rewriting its data. A
 * row unselected by the input remains unselected in the result.
 *
 * @since 3.0.0
 */
@Evolving
public interface PredicateEvaluator extends AutoCloseable {
  /**
   * Evaluate the predicate and return the input data with a narrowed selection.
   *
   * <p>The output has the same data, row count, and row order as {@code input}. A row unselected in
   * the input remains unselected. The lifetime is declared independently for each result batch.
   *
   * @param input input data and existing selection
   * @return the input data with the predicate selection applied
   */
  FilteredColumnarBatch eval(FilteredColumnarBatch input);

  @Override
  default void close() {}
}
