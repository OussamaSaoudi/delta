package io.delta.read;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.expressions.RustExpressionEvaluator;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.data.SelectionColumnVector;
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.util.PartitionUtils;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.StructField;
import io.delta.kernel.utils.CloseableIterator;
import kernel.oxidized_java.RustScanFileRow;
import kernel.oxidized_java.RustScanFileState;

import java.io.IOException;
import java.util.Optional;


public class Transformer {
    static CloseableIterator<FilteredColumnarBatch> transformPhysicalData(
            Engine engine, RustScanFileState state, RustScanFileRow scanFile, CloseableIterator<ColumnarBatch> physicalDataIter)
            throws IOException {
        return new CloseableIterator<FilteredColumnarBatch>() {

            // initialized as part of init()

            RoaringBitmapArray currBitmap = null;
            DeletionVectorDescriptor currDV = null;

            @Override
            public void close() throws IOException {
                physicalDataIter.close();
            }

            @Override
            public boolean hasNext() {
                return physicalDataIter.hasNext();
            }


            @Override
            public FilteredColumnarBatch next() {
                ColumnarBatch nextDataBatch = physicalDataIter.next();

                Optional<DeletionVectorDescriptor> dv = scanFile.dvInfo;

                int rowIndexOrdinal =
                        nextDataBatch.getSchema().indexOf(StructField.METADATA_ROW_INDEX_COLUMN_NAME);

                // Get the selectionVector if DV is present
                Optional<ColumnVector> selectionVector;
                if (!dv.isPresent()) {
                    selectionVector = Optional.empty();
                } else {
                    if (rowIndexOrdinal == -1) {
                        throw new IllegalArgumentException(
                                "Row index column is not " + "present in the data read from the Parquet file.");
                    }
                    Tuple2<DeletionVectorDescriptor, RoaringBitmapArray> dvInfo =
                            DeletionVectorUtils.loadNewDvAndBitmap(engine, state.tableRoot, dv.get());
                    this.currDV = dvInfo._1;
                    this.currBitmap = dvInfo._2;
                    ColumnVector rowIndexVector = nextDataBatch.getColumnVector(rowIndexOrdinal);
                    selectionVector = Optional.of(new SelectionColumnVector(currBitmap, rowIndexVector));
                }
                if (rowIndexOrdinal != -1) {
                    nextDataBatch = nextDataBatch.withDeletedColumnAt(rowIndexOrdinal);
                }

                if (scanFile.transform.isPresent()) {
                    RustExpressionEvaluator evaluator = new RustExpressionEvaluator(state.readSchema, scanFile.transform.get(), state.logicalSchema);
                    nextDataBatch = evaluator.eval(nextDataBatch);
                }


                return new FilteredColumnarBatch(nextDataBatch, selectionVector);
            }
        };

    }
}
