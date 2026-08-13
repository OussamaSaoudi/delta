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

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.internal.plans.Values;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Optional;

/** Executes an inline {@link Values} relation as one Kernel Java batch. */
final class ValuesExecutor {
  private ValuesExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(Values values) {
    requireNonNull(values, "values is null");
    return singletonCloseableIterator(
            new DefaultRowBasedColumnarBatch(values.getSchema(), values.getRows()))
        .map(data -> new FilteredColumnarBatch(data, Optional.empty()));
  }
}
