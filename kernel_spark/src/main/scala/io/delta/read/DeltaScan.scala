package io.delta.read

import io.delta.kernel.defaults.internal.json.JsonUtils
import org.apache.spark.sql.types.{StructType => SparkStructType}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan => SparkScan}
import org.apache.spark.sql.types.StructType
import kernel.oxidized_java.{DefaultPlanExecutor, Scan => OxidizedScan, Snapshot => OxidizedSnapshot}

class DeltaScan(
    val scan: OxidizedScan,
    snapshot: OxidizedSnapshot,
    executor: DefaultPlanExecutor,
    sparkReadSchema: SparkStructType)
    extends SparkScan
    with Batch {
  import DeltaScan._

  // Serialize scan state for executors using RustScanFileState format
  // Note: Predicates are used for data skipping (file-level) via the Rust scan builder,
  // not for row-level filtering. Spark will apply row filters after reading.
  private val serializedScanState = {
    val tableSchema = snapshot.getSchema() // This is Kernel StructType
    val kernelReadSchema = io.delta.SchemaUtils.convertSparkSchemaToKernelSchema(sparkReadSchema)
    val emptyPartitionColumns = new java.util.ArrayList[String]()
    val scanState = new kernel.oxidized_java.RustScanFileState(
      tableSchema,        // logicalSchema (table schema)
      kernelReadSchema,   // readSchema (physical schema with only selected columns)
      emptyPartitionColumns,
      snapshot.getTableRoot()
    )
    scanState.toJson()
  }

  /** Get the Kernel ScanFiles ColumnarBatchIter and convert to [[DeltaInputPartition]] array. */
  private lazy val planPartitions: Array[InputPartition] = {
    println(s"[STATE MACHINE] DeltaScan.planPartitions called - executing NEW state machine scan")
    val scanFileAsInputPartitionBuffer = scala.collection.mutable.ArrayBuffer[DeltaInputPartition]()

    // Execute scan to get FilteredColumnarBatch results
    println(s"[STATE MACHINE] Calling scan.execute() to get scan file metadata iterator")
    val scanResults = scan.execute()
    println(s"[STATE MACHINE] Got scan results iterator, starting to process batches")
    
    try {
      var batchCount = 0
      var totalRows = 0
      scanResults.forEachRemaining { filteredColumnarBatch =>
        batchCount += 1
        val batch = filteredColumnarBatch.getData()
        val rows = batch.getRows
        
        var rowCount = 0
        rows.forEachRemaining { row =>
          // The Rust state machine returns Delta log actions with both "add" and "remove" fields
          // We only care about rows where "add" is not null
          val addOrdinal = batch.getSchema.indexOf("add")
          if (addOrdinal >= 0 && !row.isNullAt(addOrdinal)) {
            rowCount += 1
            totalRows += 1
            // Extract just the "add" struct from the row
            val addStruct = row.getStruct(addOrdinal)
            val serializedScanFileRow = JsonUtils.rowToJson(addStruct)
            logger.info(s"serializedScanFileRow: $serializedScanFileRow")
            val inputPartition = DeltaInputPartition(serializedScanFileRow, serializedScanState)
            scanFileAsInputPartitionBuffer += inputPartition
          }
        }
        println(s"[STATE MACHINE] Processed batch $batchCount with $rowCount rows")
      }

      println(s"[STATE MACHINE] Scan execution complete: $batchCount batches, $totalRows total scan files")
      println(s"[STATE MACHINE] Returning ${scanFileAsInputPartitionBuffer.length} input partitions for Spark executors")
    } finally {
      // CRITICAL: Close the iterator to free Rust resources
      println(s"[STATE MACHINE] Closing scan results iterator")
      scanResults.close()
      println(s"[STATE MACHINE] Scan results iterator closed")
    }
    
    scanFileAsInputPartitionBuffer.toArray
  }

  /////////////////////////
  // SparkScan Overrides //
  /////////////////////////

  override def readSchema(): StructType = sparkReadSchema

  override def toBatch: Batch = this

  /////////////////////
  // Batch Overrides //
  /////////////////////

  // A physical representation of a data source scan for batch queries. This interface is used to
  // provide physical information, like how many partitions the scanned data has, and how to read
  // records from the partitions.

  /**
   * Returns a list of input partitions. Each InputPartition represents a data split that can be
   * processed by one Spark task. The number of input partitions returned here is the same as the
   * number of RDD partitions this scan outputs.
   *
   * If the Scan supports filter pushdown, this Batch is likely configured with a filter and is
   * responsible for creating splits for that filter, which is not a full scan.
   *
   * This method will be called only once during a data source scan, to launch one Spark job.
   */
  override def planInputPartitions(): Array[InputPartition] = planPartitions

  override def createReaderFactory(): PartitionReaderFactory = {
    new DeltaReaderFactory()
  }
}

object DeltaScan {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/** Serialized and sent from the Driver to the Executors */
case class DeltaInputPartition(serializedScanFileRow: String, serializedScanState: String)
    extends InputPartition with Serializable
