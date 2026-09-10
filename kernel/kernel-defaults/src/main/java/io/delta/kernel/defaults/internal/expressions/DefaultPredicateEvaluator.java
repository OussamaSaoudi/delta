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
package io.delta.kernel.defaults.internal.expressions;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.StructType;

/**
 * Default implementation of {@link PredicateEvaluator}. It makes use of the {@link
 * DefaultExpressionEvaluator} for the predicate value and intersects it with existing selection.
 */
public class DefaultPredicateEvaluator implements PredicateEvaluator {
  private final DefaultExpressionEvaluator expressionEvaluator;

  public DefaultPredicateEvaluator(StructType inputSchema, Predicate predicate) {
    this.expressionEvaluator =
        new DefaultExpressionEvaluator(inputSchema, predicate, BooleanType.BOOLEAN);
  }

  @Override
  public FilteredColumnarBatch eval(FilteredColumnarBatch input) {
    ColumnVector predicate = expressionEvaluator.eval(input.getData());
    return input.withSelectionVector(predicate, input.getLifetime());
  }

  @Override
  public void close() {
    expressionEvaluator.close();
  }
}
