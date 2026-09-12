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
package io.delta.kernel.execution;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.plans.PlanNode.FileScan;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.List;

/** Engine operations required by plan execution. */
public interface PlanEngine {
  /** A reusable, single-threaded batch transformation bound to one plan expression. */
  interface BatchEvaluator extends AutoCloseable {
    ColumnarBatch eval(ColumnarBatch input);

    @Override
    default void close() {}
  }

  /** Binds a struct expression producing one aligned row of {@code outputSchema} per input row. */
  BatchEvaluator bind(StructType inputSchema, Expression expression, StructType outputSchema);

  /** Binds a predicate that returns only matching rows. */
  BatchEvaluator bind(StructType inputSchema, Predicate predicate);

  /** Returns the rows selected by an aligned keep mask in the engine's native representation. */
  ColumnarBatch filter(ColumnarBatch input, boolean[] keep);

  /** Retains one row prefix for hash state in this engine's native representation. */
  Row retainRow(Row input, StructType schema);

  /** Retains one value as a one-field row in this engine's native representation. */
  Row retainValue(Row input, int ordinal, StructType outputSchema);

  /** Creates one LONG value in this engine's native representation. */
  Row longValue(long value, StructType outputSchema);

  /** Appends aligned columns using this engine's native row representation. */
  ColumnarBatch appendColumns(
      ColumnarBatch input, StructType outputSchema, List<List<Row>> columns);

  CloseableIterator<ColumnarBatch> scan(FileScan scan);
}
