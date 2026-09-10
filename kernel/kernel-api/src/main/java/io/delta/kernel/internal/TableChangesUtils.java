/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.internal;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineException;

import io.delta.kernel.CommitActions;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.StructExpression;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Arrays;
import java.util.Optional;

/** Utility class for table changes operations. */
public class TableChangesUtils {

  /** Column name for the version metadata column added to getActions results. */
  public static final String VERSION_COLUMN_NAME = "version";

  /** Column name for the timestamp metadata column added to getActions results. */
  public static final String TIMESTAMP_COLUMN_NAME = "timestamp";

  /** StructField for the version metadata column. */
  private static final StructField VERSION_STRUCT_FIELD =
      new StructField(VERSION_COLUMN_NAME, LongType.LONG, false);

  /** StructField for the timestamp metadata column. */
  private static final StructField TIMESTAMP_STRUCT_FIELD =
      new StructField(TIMESTAMP_COLUMN_NAME, LongType.LONG, false);

  private TableChangesUtils() {}

  /**
   * Adds version and timestamp columns to a columnar batch.
   *
   * <p>The version and timestamp columns are added as the first two columns in the batch.
   *
   * @param engine the engine for expression evaluation
   * @param batch the original batch
   * @param version the version value to add
   * @param timestamp the timestamp value to add
   * @return a new batch with version and timestamp columns prepended
   */
  public static ColumnarBatch addVersionAndTimestampColumns(
      Engine engine, ColumnarBatch batch, long version, long timestamp) {
    StructType schemaForEval = batch.getSchema();

    StructType metadataSchema =
        new StructType(Arrays.asList(VERSION_STRUCT_FIELD, TIMESTAMP_STRUCT_FIELD));
    ExpressionEvaluator metadataGenerator =
        wrapEngineException(
            () ->
                engine
                    .getExpressionHandler()
                    .getEvaluator(
                        schemaForEval,
                        new StructExpression(
                            Arrays.asList(Literal.ofLong(version), Literal.ofLong(timestamp))),
                        metadataSchema),
            "Get the expression evaluator for commit metadata");

    ColumnarBatch metadata =
        wrapEngineException(
            () ->
                metadataGenerator
                    .eval(new FilteredColumnarBatch(batch, Optional.empty()))
                    .getData(),
            "Evaluating the commit metadata expression");

    return batch
        .withNewColumn(0, VERSION_STRUCT_FIELD, metadata.getColumnVector(0))
        .withNewColumn(1, TIMESTAMP_STRUCT_FIELD, metadata.getColumnVector(1));
  }

  /**
   * Flattens an iterator of CommitActions into an iterator of ColumnarBatch, adding version and
   * timestamp columns to each batch.
   *
   * @param engine the engine for expression evaluation
   * @param commits the iterator of CommitActions to flatten
   * @return an iterator of ColumnarBatch with version and timestamp columns added
   */
  public static CloseableIterator<ColumnarBatch> flattenCommitsAndAddMetadata(
      Engine engine, CloseableIterator<CommitActions> commits) {
    CloseableIterator<CloseableIterator<ColumnarBatch>> nestedIterator =
        commits.map(
            commit -> {
              long version = commit.getVersion();
              long timestamp = commit.getTimestamp();
              CloseableIterator<ColumnarBatch> actions = commit.getActions();

              // Map each batch to add version and timestamp columns
              return actions.map(
                  batch -> addVersionAndTimestampColumns(engine, batch, version, timestamp));
            });

    return Utils.flatten(nestedIterator);
  }
}
