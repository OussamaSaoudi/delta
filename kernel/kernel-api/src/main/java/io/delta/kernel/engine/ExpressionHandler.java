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
package io.delta.kernel.engine;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.types.StructType;

/**
 * Provides expression evaluation capability to Delta Kernel. Delta Kernel can use this client to
 * evaluate predicate on partition filters, fill up partition column values and any computation on
 * data using {@link Expression}s.
 *
 * @since 3.0.0
 */
@Evolving
public interface ExpressionHandler {

  /**
   * Creates an evaluator for a struct-valued expression that produces one output row for each input
   * row.
   *
   * @param inputSchema input data schema
   * @param expression expression to evaluate
   * @param outputSchema expected fields in each output row
   * @return an evaluator bound to the input schema, expression, and output schema
   */
  ExpressionEvaluator getEvaluator(
      StructType inputSchema, Expression expression, StructType outputSchema);

  /**
   * Creates an evaluator that applies the predicate by narrowing a batch's selection.
   *
   * @param inputSchema schema of the data referred to by the predicate
   * @param predicate predicate expression to evaluate
   * @return an evaluator bound to the input schema and predicate
   */
  PredicateEvaluator getPredicateEvaluator(StructType inputSchema, Predicate predicate);

  /**
   * Create a selection vector, a boolean type {@link ColumnVector}, on top of the range of values
   * given in <i>values</i> array.
   *
   * @param values Array of initial boolean values for the selection vector. The ownership of this
   *     array is with the caller and this method shouldn't depend on it after the call is complete.
   * @param from start index of the range, inclusive.
   * @param to end index of the range, exclusive.
   * @return A {@link ColumnVector} of {@code boolean} type values.
   */
  ColumnVector createSelectionVector(boolean[] values, int from, int to);
}
