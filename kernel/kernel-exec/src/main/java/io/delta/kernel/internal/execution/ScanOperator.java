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
package io.delta.kernel.internal.execution;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.plans.ScanJson;
import io.delta.kernel.plans.ScanParquet;
import io.delta.kernel.utils.CloseableIterator;

/** Delegates complete scan requests to the host engine. */
final class ScanOperator {
  private ScanOperator() {}

  static CloseableIterator<FilteredColumnarBatch> open(ScanJson scan, Engine engine) {
    return requireNonNull(
        engine.getJsonHandler().readJsonFiles(scan), "JSON scan iterator is null");
  }

  static CloseableIterator<FilteredColumnarBatch> open(ScanParquet scan, Engine engine) {
    return requireNonNull(
        engine.getParquetHandler().readParquetFiles(scan), "Parquet scan iterator is null");
  }
}
