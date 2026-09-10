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
package io.delta.kernel.data;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Optional;

/**
 * Represents a filtered version of {@link ColumnarBatch}. Contains original {@link ColumnarBatch}
 * with an optional selection vector to select only a subset of rows for the original columnar
 * batch.
 *
 * <p>The selection vector is of type boolean and has the same size as the data in the corresponding
 * {@link ColumnarBatch}. For each row index, a value of true in the selection vector indicates the
 * row at the same index in the data {@link ColumnarBatch} is valid; a value of false indicates the
 * row should be ignored. If there is no selection vector then all the rows are valid.
 *
 * @since 3.0.0
 */
@Evolving
public class FilteredColumnarBatch {
  /** How long the data reachable from a returned batch remains valid. */
  public enum Lifetime {
    /** Valid only until the producing iterator or evaluator advances or closes. */
    BORROWED,

    /** Immutable, concurrently readable, and valid while reachable. */
    OWNED
  }

  private final ColumnarBatch data;
  private final Optional<ColumnVector> selectionVector;
  private final Optional<String> filePath;
  private final Optional<Integer> preComputedNumSelectedRows;
  private final Lifetime lifetime;

  // TODO: use static factory for constructors
  public FilteredColumnarBatch(ColumnarBatch data, Optional<ColumnVector> selectionVector) {
    this(data, selectionVector, Lifetime.BORROWED);
  }

  public FilteredColumnarBatch(
      ColumnarBatch data, Optional<ColumnVector> selectionVector, Lifetime lifetime) {
    this.data = requireNonNull(data, "data is null");
    this.selectionVector = requireNonNull(selectionVector, "selectionVector is null");
    validateSelectionVector(data, selectionVector);
    this.filePath = Optional.empty();
    this.preComputedNumSelectedRows =
        !selectionVector.isPresent() ? Optional.of(data.getSize()) : Optional.empty();
    this.lifetime = requireNonNull(lifetime, "lifetime is null");
  }

  public FilteredColumnarBatch(
      ColumnarBatch data,
      Optional<ColumnVector> selectionVector,
      String filePath,
      int preComputedNumSelectedRows) {
    this(data, selectionVector, filePath, preComputedNumSelectedRows, Lifetime.BORROWED);
  }

  public FilteredColumnarBatch(
      ColumnarBatch data,
      Optional<ColumnVector> selectionVector,
      String filePath,
      int preComputedNumSelectedRows,
      Lifetime lifetime) {
    this.data = requireNonNull(data, "data is null");
    this.selectionVector = requireNonNull(selectionVector, "selectionVector is null");
    validateSelectionVector(data, selectionVector);
    checkArgument(
        selectionVector.isPresent() || preComputedNumSelectedRows == data.getSize(),
        "Invalid precomputedNumSelectedRows: must be equal to batch size "
            + "when selectionVector is empty.");
    checkArgument(
        preComputedNumSelectedRows >= 0 && preComputedNumSelectedRows <= data.getSize(),
        "Invalid precomputedNumSelectedRows: "
            + "must be no less than 0 and no larger than batch size.");
    this.filePath = Optional.of(requireNonNull(filePath, "filePath is null"));
    this.preComputedNumSelectedRows = Optional.of(preComputedNumSelectedRows);
    this.lifetime = requireNonNull(lifetime, "lifetime is null");
  }

  private FilteredColumnarBatch(
      ColumnarBatch data,
      Optional<ColumnVector> selectionVector,
      Optional<String> filePath,
      Optional<Integer> preComputedNumSelectedRows,
      Lifetime lifetime) {
    this.data = requireNonNull(data, "data is null");
    this.selectionVector = requireNonNull(selectionVector, "selectionVector is null");
    validateSelectionVector(data, selectionVector);
    this.filePath = requireNonNull(filePath, "filePath is null");
    this.preComputedNumSelectedRows =
        requireNonNull(preComputedNumSelectedRows, "preComputedNumSelectedRows is null");
    this.lifetime = requireNonNull(lifetime, "lifetime is null");
  }

  /**
   * Return the data as {@link ColumnarBatch}. Not all rows in the data are valid for this result.
   * An optional <i>selectionVector</i> determines which rows are selected. If there is no selection
   * vector that means all rows in this columnar batch are valid for this result.
   *
   * @return all the data read from the file
   */
  public ColumnarBatch getData() {
    return data;
  }

  /**
   * Returns the file path from which the data originates, if available.
   *
   * <p>Note: The file path may not be present. It is only set if explicitly provided in the
   * constructor.
   *
   * @return an {@link Optional} containing the file path if available, otherwise an empty Optional
   */
  public Optional<String> getFilePath() {
    return filePath;
  }

  /**
   * Optional selection vector containing one entry for each row in <i>data</i> indicating whether a
   * row is selected or not selected. If there is no selection vector then all the rows are valid.
   *
   * @return an optional {@link ColumnVector} indicating which rows are valid
   */
  public Optional<ColumnVector> getSelectionVector() {
    return selectionVector;
  }

  /** Returns the lifetime of this batch and all data reachable from it. */
  public Lifetime getLifetime() {
    return lifetime;
  }

  /** Returns whether the row at {@code rowId} is selected by this batch. */
  public boolean isSelected(int rowId) {
    checkArgument(rowId >= 0 && rowId < data.getSize(), "Invalid rowId: %s", rowId);
    return !selectionVector.isPresent() || selected(selectionVector.get(), rowId);
  }

  /**
   * Returns a filtered batch over replacement data while preserving this batch's selection and
   * metadata.
   *
   * <p>This is useful for projections that replace, reorder, or drop columns without changing the
   * physical row set. The replacement must therefore have the same number of rows.
   */
  public FilteredColumnarBatch withData(ColumnarBatch replacement) {
    requireNonNull(replacement, "replacement is null");
    checkArgument(
        replacement.getSize() == data.getSize(),
        "Replacement batch size %s does not match existing batch size %s",
        replacement.getSize(),
        data.getSize());
    return new FilteredColumnarBatch(
        replacement, selectionVector, filePath, preComputedNumSelectedRows, lifetime);
  }

  /** Returns a replacement-data batch with an explicitly declared result lifetime. */
  public FilteredColumnarBatch withData(ColumnarBatch replacement, Lifetime resultLifetime) {
    requireNonNull(replacement, "replacement is null");
    checkArgument(
        replacement.getSize() == data.getSize(),
        "Replacement batch size %s does not match existing batch size %s",
        replacement.getSize(),
        data.getSize());
    return new FilteredColumnarBatch(
        replacement,
        selectionVector,
        filePath,
        preComputedNumSelectedRows,
        requireNonNull(resultLifetime, "resultLifetime is null"));
  }

  /**
   * Returns a batch over owned replacements while preserving this batch's file and row-count
   * metadata.
   *
   * <p>This is the retention counterpart to {@link #withData(ColumnarBatch, Lifetime)}. Both the
   * physical rows and selection are replaced so an owned copy preserves the original batch shape
   * without borrowing either vector.
   */
  public FilteredColumnarBatch withDataAndSelection(
      ColumnarBatch replacement,
      Optional<ColumnVector> replacementSelection,
      Lifetime resultLifetime) {
    requireNonNull(replacement, "replacement is null");
    checkArgument(
        replacement.getSize() == data.getSize(),
        "Replacement batch size %s does not match existing batch size %s",
        replacement.getSize(),
        data.getSize());
    return new FilteredColumnarBatch(
        replacement,
        requireNonNull(replacementSelection, "replacementSelection is null"),
        filePath,
        preComputedNumSelectedRows,
        requireNonNull(resultLifetime, "resultLifetime is null"));
  }

  /**
   * Returns a batch whose selected rows are the intersection of the current and additional
   * selection vectors.
   *
   * <p>A null entry in either vector is treated as unselected. The returned selection vector is a
   * borrowed, lazy view; closing it does not close either input vector.
   */
  public FilteredColumnarBatch withSelectionVector(ColumnVector additionalSelectionVector) {
    return withSelectionVector(additionalSelectionVector, Lifetime.BORROWED);
  }

  /** Returns an intersected selection with an explicitly declared result lifetime. */
  public FilteredColumnarBatch withSelectionVector(
      ColumnVector additionalSelectionVector, Lifetime resultLifetime) {
    requireNonNull(additionalSelectionVector, "additionalSelectionVector is null");
    validateSelectionVector(data, Optional.of(additionalSelectionVector));
    ColumnVector combined = new CombinedSelectionVector(selectionVector, additionalSelectionVector);
    return new FilteredColumnarBatch(
        data,
        Optional.of(combined),
        filePath,
        Optional.empty(),
        requireNonNull(resultLifetime, "resultLifetime is null"));
  }

  /**
   * Iterator of rows that survived the filter.
   *
   * @return Closeable iterator of rows that survived the filter. It is responsibility of the caller
   *     to the close the iterator.
   */
  public CloseableIterator<Row> getRows() {
    if (!selectionVector.isPresent()) {
      return data.getRows();
    }
    int[] rowId = {0};
    return data.getRows().filter(ignored -> isSelected(rowId[0]++));
  }

  /**
   * @return an {@link Optional} containing the pre-computed number of selected rows, if available.
   *     <p>If present, this value was computed ahead of time and can be used without incurring any
   *     additional cost. This occurs in two cases:
   *     <ul>
   *       <li>When the selection vector is absent, which implies that all rows are selected; in
   *           this case, the number of selected rows is equal to the batch size.
   *       <li>When the number of selected rows was explicitly pre-computed and passed in.
   *     </ul>
   *     <p>If empty, the caller must compute the number of selected rows manually from the
   *     selection vector.
   */
  public Optional<Integer> getPreComputedNumSelectedRows() {
    return preComputedNumSelectedRows;
  }

  private static void validateSelectionVector(
      ColumnarBatch data, Optional<ColumnVector> selectionVector) {
    selectionVector.ifPresent(
        vector -> {
          checkArgument(
              BooleanType.BOOLEAN.equals(vector.getDataType()),
              "Selection vector must have boolean type, got %s",
              vector.getDataType());
          checkArgument(
              vector.getSize() == data.getSize(),
              "Selection vector size %s does not match batch size %s",
              vector.getSize(),
              data.getSize());
        });
  }

  private static boolean selected(ColumnVector vector, int rowId) {
    return !vector.isNullAt(rowId) && vector.getBoolean(rowId);
  }

  private static final class CombinedSelectionVector implements ColumnVector {
    private final Optional<ColumnVector> left;
    private final ColumnVector right;

    private CombinedSelectionVector(Optional<ColumnVector> left, ColumnVector right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public DataType getDataType() {
      return BooleanType.BOOLEAN;
    }

    @Override
    public int getSize() {
      return right.getSize();
    }

    @Override
    public void close() {
      // This vector borrows its inputs.
    }

    @Override
    public boolean isNullAt(int rowId) {
      validateRowId(rowId);
      return false;
    }

    @Override
    public boolean getBoolean(int rowId) {
      validateRowId(rowId);
      return (!left.isPresent() || selected(left.get(), rowId)) && selected(right, rowId);
    }

    private void validateRowId(int rowId) {
      checkArgument(rowId >= 0 && rowId < getSize(), "Invalid rowId: %s", rowId);
    }
  }
}
