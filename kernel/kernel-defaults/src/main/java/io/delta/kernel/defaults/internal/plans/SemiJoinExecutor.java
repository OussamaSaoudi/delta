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

import static io.delta.kernel.defaults.internal.plans.PlanValueUtils.canonicalize;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector;
import io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.internal.plans.PlanSchemaUtils;
import io.delta.kernel.internal.plans.SemiJoin;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Executes a {@link SemiJoin} by materializing build keys and lazily filtering probe batches. */
final class SemiJoinExecutor {
  private SemiJoinExecutor() {}

  static CloseableIterator<FilteredColumnarBatch> execute(
      SemiJoin join,
      StructType probeSchema,
      StructType buildSchema,
      CloseableIterator<FilteredColumnarBatch> probe,
      CloseableIterator<FilteredColumnarBatch> build) {
    requireNonNull(join, "join is null");
    requireNonNull(probeSchema, "probeSchema is null");
    requireNonNull(buildSchema, "buildSchema is null");
    requireNonNull(probe, "probe is null");
    requireNonNull(build, "build is null");

    join.getOutputSchema(Arrays.asList(probeSchema, buildSchema));
    KeyProjection probeKeys = KeyProjection.bind(probeSchema, join.getProbeKeys(), "probe");
    KeyProjection buildKeys = KeyProjection.bind(buildSchema, join.getBuildKeys(), "build");
    Set<List<Object>> materialized = materializeBuild(buildSchema, buildKeys, build, probe);

    return probe.map(
        batch -> filterProbe(join.isInverted(), probeSchema, probeKeys, materialized, batch));
  }

  private static Set<List<Object>> materializeBuild(
      StructType schema,
      KeyProjection projection,
      CloseableIterator<FilteredColumnarBatch> build,
      CloseableIterator<FilteredColumnarBatch> probe) {
    Set<List<Object>> keys = new HashSet<>();
    try {
      while (build.hasNext()) {
        FilteredColumnarBatch batch = requireNonNull(build.next(), "build batch is null");
        validateSchema("build", schema, batch.getData());
        try (EvaluatedKeys values = projection.evaluate(batch.getData())) {
          for (int rowId = 0; rowId < batch.getData().getSize(); rowId++) {
            if (isSelected(batch, rowId)) {
              keys.add(values.keyAt(rowId));
            }
          }
        }
      }
    } catch (RuntimeException | Error failure) {
      closeAfterFailure(failure, build, probe);
      throw failure;
    }

    try {
      Utils.closeCloseables(build);
    } catch (RuntimeException | Error failure) {
      closeAfterFailure(failure, probe);
      throw failure;
    }
    return keys;
  }

  private static FilteredColumnarBatch filterProbe(
      boolean inverted,
      StructType schema,
      KeyProjection projection,
      Set<List<Object>> buildKeys,
      FilteredColumnarBatch batch) {
    requireNonNull(batch, "probe batch is null");
    ColumnarBatch data = batch.getData();
    validateSchema("probe", schema, data);
    boolean[] selected = new boolean[data.getSize()];
    try (EvaluatedKeys values = projection.evaluate(data)) {
      for (int rowId = 0; rowId < data.getSize(); rowId++) {
        if (isSelected(batch, rowId)) {
          selected[rowId] = inverted != buildKeys.contains(values.keyAt(rowId));
        }
      }
    }
    return batch.withSelectionVector(
        new DefaultBooleanVector(selected.length, Optional.empty(), selected));
  }

  private static void validateSchema(String side, StructType expected, ColumnarBatch data) {
    requireNonNull(data, side + " batch data is null");
    if (!expected.equals(data.getSchema())) {
      throw new IllegalArgumentException(
          "SemiJoin "
              + side
              + " batch schema "
              + data.getSchema()
              + " does not match expected schema "
              + expected);
    }
  }

  private static boolean isSelected(FilteredColumnarBatch batch, int rowId) {
    Optional<ColumnVector> selection = batch.getSelectionVector();
    return !selection.isPresent()
        || (!selection.get().isNullAt(rowId) && selection.get().getBoolean(rowId));
  }

  private static void closeAfterFailure(Throwable failure, AutoCloseable... closeables) {
    try {
      Utils.closeCloseables(closeables);
    } catch (RuntimeException | Error closeFailure) {
      failure.addSuppressed(closeFailure);
    }
  }

  private static final class KeyProjection {
    private final List<ExpressionEvaluator> evaluators;
    private final List<DataType> types;

    private KeyProjection(List<ExpressionEvaluator> evaluators, List<DataType> types) {
      this.evaluators = evaluators;
      this.types = types;
    }

    static KeyProjection bind(StructType schema, List<Column> columns, String side) {
      List<ExpressionEvaluator> evaluators = new ArrayList<>(columns.size());
      List<DataType> types = new ArrayList<>(columns.size());
      for (Column column : columns) {
        StructField field =
            PlanSchemaUtils.resolveField(schema, column, "SemiJoin " + side + " key");
        DataType type = field.getDataType();
        PlanValueUtils.validateCanonicalizable(type, "SemiJoin " + side + " key", true);
        evaluators.add(new DefaultExpressionEvaluator(schema, column, type));
        types.add(type);
      }
      return new KeyProjection(evaluators, types);
    }

    EvaluatedKeys evaluate(ColumnarBatch data) {
      List<ColumnVector> vectors = new ArrayList<>(evaluators.size());
      try {
        for (ExpressionEvaluator evaluator : evaluators) {
          vectors.add(evaluator.eval(data));
        }
        return new EvaluatedKeys(vectors, types);
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(failure, vectors.toArray(new AutoCloseable[0]));
        throw failure;
      }
    }
  }

  private static final class EvaluatedKeys implements AutoCloseable {
    private final List<ColumnVector> vectors;
    private final List<DataType> types;

    private EvaluatedKeys(List<ColumnVector> vectors, List<DataType> types) {
      this.vectors = vectors;
      this.types = types;
    }

    List<Object> keyAt(int rowId) {
      List<Object> key = new ArrayList<>(vectors.size());
      for (int index = 0; index < vectors.size(); index++) {
        key.add(canonicalize(vectors.get(index), types.get(index), rowId));
      }
      return key;
    }

    @Override
    public void close() {
      Utils.closeCloseables(vectors.toArray(new AutoCloseable[0]));
    }
  }
}
