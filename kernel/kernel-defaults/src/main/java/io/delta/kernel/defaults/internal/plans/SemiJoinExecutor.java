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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.plans.SemiJoin;
import io.delta.kernel.internal.util.ColumnBinding;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Materializes build keys, then lazily filters probe batches. */
final class SemiJoinExecutor {
  private SemiJoinExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(
      SemiJoin join,
      StructType probeSchema,
      StructType buildSchema,
      CloseableIterator<FilteredColumnarBatch> probe,
      CloseableIterator<FilteredColumnarBatch> build) {
    requireNonNull(join, "join is null")
        .getOutputSchema(Arrays.asList(probeSchema, buildSchema));
    requireNonNull(probe, "probe is null");
    requireNonNull(build, "build is null");
    BoundKeys probeKeys = new BoundKeys(probeSchema, join.getProbeKeys());
    BoundKeys buildKeyColumns = new BoundKeys(buildSchema, join.getBuildKeys());
    PlanValueUtils.KeyMap<Boolean> buildKeys = new PlanValueUtils.KeyMap<>(probeKeys.types);
    materializeBuild(buildSchema, buildKeyColumns, buildKeys, build, probe);
    return probe.map(
        batch ->
            filterProbe(
                join.isInverted(), probeSchema, probeKeys, buildKeys, batch));
  }

  private static void materializeBuild(
      StructType schema,
      BoundKeys projection,
      PlanValueUtils.KeyMap<Boolean> keys,
      CloseableIterator<FilteredColumnarBatch> build,
      CloseableIterator<FilteredColumnarBatch> probe) {
    try {
      while (build.hasNext()) {
        FilteredColumnarBatch batch = requireNonNull(build.next(), "build batch is null");
        validateSchema("build", schema, batch.getData());
        List<ColumnVector> vectors = projection.getVectors(batch.getData());
        for (int rowId = 0; rowId < batch.getData().getSize(); rowId++) {
          if (batch.isSelected(rowId)) {
            keys.getOrInsert(vectors, rowId, ignored -> Boolean.TRUE);
          }
        }
      }
    } catch (RuntimeException | Error failure) {
      Utils.closeCloseablesAndAddSuppressed(failure, build, probe);
      throw failure;
    }
    try {
      Utils.closeCloseables(build);
    } catch (RuntimeException | Error failure) {
      Utils.closeCloseablesAndAddSuppressed(failure, probe);
      throw failure;
    }
  }

  private static FilteredColumnarBatch filterProbe(
      boolean inverted,
      StructType schema,
      BoundKeys projection,
      PlanValueUtils.KeyMap<Boolean> buildKeys,
      FilteredColumnarBatch batch) {
    requireNonNull(batch, "probe batch is null");
    ColumnarBatch data = batch.getData();
    validateSchema("probe", schema, data);
    boolean[] selected = new boolean[data.getSize()];
    List<ColumnVector> vectors = projection.getVectors(data);
    for (int rowId = 0; rowId < data.getSize(); rowId++) {
      selected[rowId] =
          batch.isSelected(rowId) && inverted != buildKeys.contains(vectors, rowId);
    }
    return batch.withSelectionVector(
        new DefaultBooleanVector(selected.length, Optional.empty(), selected));
  }

  private static void validateSchema(String side, StructType expected, ColumnarBatch data) {
    if (!expected.equals(data.getSchema())) {
      throw new IllegalArgumentException(
          "SemiJoin " + side + " batch schema does not match " + expected);
    }
  }

  private static final class BoundKeys {
    private final List<ColumnBinding> bindings = new ArrayList<>();
    private final List<DataType> types = new ArrayList<>();

    private BoundKeys(StructType schema, List<Column> columns) {
      for (Column column : columns) {
        ColumnBinding binding = ColumnBinding.resolve(schema, column);
        PlanValueUtils.requireGroupType(binding.getDataType());
        bindings.add(binding);
        types.add(binding.getDataType());
      }
    }

    private List<ColumnVector> getVectors(ColumnarBatch data) {
      List<ColumnVector> vectors = new ArrayList<>(bindings.size());
      for (ColumnBinding binding : bindings) {
        vectors.add(binding.getVector(data));
      }
      return vectors;
    }
  }
}
