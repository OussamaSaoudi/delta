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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.data.RowBackedColumnarBatch;
import io.delta.kernel.internal.util.RowKernels;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Materialization at execution lifetime boundaries. */
public final class OwnedBatches {
  private OwnedBatches() {}

  public static FilteredColumnarBatch retain(FilteredColumnarBatch batch) {
    if (batch.getLifetime() == FilteredColumnarBatch.Lifetime.OWNED) {
      return batch;
    }

    ArrayList<Row> rows = new ArrayList<>(batch.getData().getSize());
    try (CloseableIterator<Row> inputRows = batch.getData().getRows()) {
      while (inputRows.hasNext()) {
        rows.add(RowKernels.materialize(inputRows.next()));
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new KernelException("Could not retain batch", e);
    }

    Optional<ColumnVector> selection =
        batch.getSelectionVector().map(OwnedBatches::retainSelection);
    return batch.withDataAndSelection(
        new RowBackedColumnarBatch(batch.getData().getSchema(), rows),
        selection,
        FilteredColumnarBatch.Lifetime.OWNED);
  }

  private static ColumnVector retainSelection(ColumnVector selection) {
    List<Object> values = new ArrayList<>(selection.getSize());
    for (int rowId = 0; rowId < selection.getSize(); rowId++) {
      values.add(RowKernels.materialize(selection, BooleanType.BOOLEAN, rowId));
    }
    return VectorUtils.buildColumnVector(values, BooleanType.BOOLEAN);
  }

  public static long estimatedBytes(FilteredColumnarBatch batch) {
    long cells = (long) batch.getData().getSize() * batch.getData().getSchema().length();
    long boundedCells = Math.max(1L, cells);
    return boundedCells > (Long.MAX_VALUE - 64L) / 16L ? Long.MAX_VALUE : 64L + boundedCells * 16L;
  }
}
