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

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.internal.plans.UnionAll;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.List;

/** Executes a {@link UnionAll} by lazily concatenating its input batch iterators. */
final class UnionAllExecutor {
  private UnionAllExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(
      List<CloseableIterator<FilteredColumnarBatch>> inputs) {
    requireNonNull(inputs, "inputs is null");
    if (inputs.isEmpty()) {
      throw new IllegalArgumentException("UnionAll requires at least one input iterator");
    }

    List<CloseableIterator<FilteredColumnarBatch>> checked = new ArrayList<>(inputs.size());
    for (int inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
      checked.add(
          requireNonNull(inputs.get(inputIndex), "input iterator " + inputIndex + " is null"));
    }

    CloseableIterator<FilteredColumnarBatch> combined = checked.get(0);
    for (int inputIndex = 1; inputIndex < checked.size(); inputIndex++) {
      combined = combined.combine(checked.get(inputIndex));
    }
    return combined;
  }
}
