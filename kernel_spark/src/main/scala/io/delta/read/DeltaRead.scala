package io.delta.read

import io.delta.data.{KernelColumnarBatchToSparkColumnarBatchWrapper, KernelRowToSparkRowWrapper}
import io.delta.engine.KernelSparkEngine
import io.delta.kernel.internal.data.ScanStateRow
import io.delta.kernel.internal.util.{JsonUtils, Utils}
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import kernel.oxidized_java.{RustScanFileRow, RustScanFileState}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Serialized and sent from the Driver to the Executors */
class DeltaReaderFactory extends PartitionReaderFactory {
  import DeltaReaderFactory._

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    logger.info("createReader")

    require(partition.isInstanceOf[DeltaInputPartition])

    new DeltaPartitionReaderOfRows(partition.asInstanceOf[DeltaInputPartition])
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    logger.info("createColumnarReader")

    require(partition.isInstanceOf[DeltaInputPartition])

    new DeltaPartitionReaderOfColumnarBatch(partition.asInstanceOf[DeltaInputPartition])
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    logger.info("supportColumnarReads")
    SparkSession.active.sparkContext.getConf
      .getBoolean("io.delta.kernel.spark.supportColumnarReads", defaultValue = true)
  }
}

object DeltaReaderFactory {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/** Created on Executor */
abstract class DeltaPartitionReader[T](deltaInputPartition: DeltaInputPartition)
    extends PartitionReader[T] {
//  //scalastyle:off
//  println("scan row: " + deltaInputPartition.serializedScanFileRow)
//  println("scan state: " + deltaInputPartition.serializedScanState)
//  // scalastyle:on

  protected val engine = KernelSparkEngine.createOnExecutor()

  protected val scanFileRow: RustScanFileRow =
    RustScanFileRow.fromJson(deltaInputPartition.serializedScanFileRow)
  protected val scanState: RustScanFileState =
    RustScanFileState.fromJson(deltaInputPartition.serializedScanState)

  // Construct absolute file path by combining table root and relative path
  protected val absoluteFilePath = {
    val tableRoot = scanState.tableRoot
    val relativePath = scanFileRow.path
    // URL-decode the path (Delta stores paths URL-encoded)
    val decodedPath = java.net.URLDecoder.decode(relativePath, "UTF-8")
    if (decodedPath.startsWith("/") || decodedPath.contains("://")) {
      // Already absolute
      decodedPath
    } else {
      // Relative path - combine with table root
      s"$tableRoot/$decodedPath"
    }
  }

  // Read Parquet files without row-level filtering
  // Predicates are only used for data skipping (file-level filtering) via the Rust scan builder
  protected val physicalRowDataIter = engine.getParquetHandler
    .readParquetFiles(
      Utils.singletonCloseableIterator(FileStatus.of(absoluteFilePath, scanFileRow.size, 0)),
      scanState.readSchema,
      java.util.Optional.empty() /* no row-level predicate - Spark filters post-read */ )

  protected val logicalRowDataColumnarBatchIter =
    Transformer.transformPhysicalData(engine, scanState, scanFileRow, physicalRowDataIter)
}

/** Created on the executor. */
class DeltaPartitionReaderOfRows(deltaInputPartition: DeltaInputPartition)
    extends DeltaPartitionReader[InternalRow](deltaInputPartition) {
  import DeltaPartitionReaderOfRows._

  logger.info("DeltaPartitionReaderOfRows constructed")

  private var rowIter: CloseableIterator[io.delta.kernel.data.Row] = null
  private var curr: io.delta.kernel.data.Row = null
  private var closed = false

  override def close(): Unit = {
    logicalRowDataColumnarBatchIter.close()
    if (rowIter != null) {
      rowIter.close()
    }
    closed = true
  }

  override def next(): Boolean = {
    if (!closed) {
      // Check if there are remaining rows in the current row iterator
      if (rowIter != null && rowIter.hasNext) {
        curr = rowIter.next()
        true
      }
      // If current batch is exhausted, fetch the next batch and reset the row iterator
      else if (logicalRowDataColumnarBatchIter.hasNext) {
        rowIter = logicalRowDataColumnarBatchIter.next().getRows
        next() // Recursively call next to process the new batch
      } else {
        false
      }
    } else {
      false
    }
  }

  override def get(): InternalRow = {
    if (curr == null) {
      throw new NoSuchElementException("No current row available; call next() first.")
    }
    new KernelRowToSparkRowWrapper(curr)
  }
}

object DeltaPartitionReaderOfRows {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

class DeltaPartitionReaderOfColumnarBatch(deltaInputPartition: DeltaInputPartition)
    extends DeltaPartitionReader[ColumnarBatch](deltaInputPartition) {
  import DeltaPartitionReaderOfColumnarBatch._

  logger.info("DeltaPartitionReaderOfColumnarBatch constructed")

  private var currentBatch: KernelColumnarBatchToSparkColumnarBatchWrapper = null
  private var closed = false

  override def next(): Boolean = {
    if (!closed) {
      if (logicalRowDataColumnarBatchIter.hasNext) {
        val kernelBatch = logicalRowDataColumnarBatchIter.next()
        currentBatch = KernelColumnarBatchToSparkColumnarBatchWrapper(kernelBatch)
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  override def get(): ColumnarBatch = {
    if (currentBatch == null) {
      throw new NoSuchElementException("No current batch available; call next() first.")
    }
    currentBatch
  }

  override def close(): Unit = {
    if (!closed) {
      logicalRowDataColumnarBatchIter.close()
      if (currentBatch != null) {
        currentBatch.close()
      }
      closed = true
    }
  }
}

object DeltaPartitionReaderOfColumnarBatch {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}
