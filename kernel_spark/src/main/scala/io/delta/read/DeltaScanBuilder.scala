package io.delta.read

import io.delta.{ExpressionUtils, SchemaUtils}
import io.delta.kernel.{Table => KernelTable}
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.expressions.{And => KernelAnd}
import io.delta.kernel.internal.ScanImpl
import io.delta.engine.KernelSparkEngine
import kernel.oxidized_java.{DefaultPlanExecutor, Snapshot => OxidizedSnapshot}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

class DeltaScanBuilder(kernelTable: KernelTable, tableEngine: KernelEngine)
    extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownV2Filters {
  import DeltaScanBuilder._

  // Create executor for state machine execution
  private val executor = new DefaultPlanExecutor(tableEngine)
  
  // Get table path and create snapshot using state machine API
  private val tablePath = kernelTable.getPath(tableEngine)
  private val snapshot = OxidizedSnapshot.forPath(tablePath, executor)
  
  private var kernelPredicate: Option[io.delta.kernel.expressions.Predicate] = Option.empty
  private var sparkSchema =
    SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema())
  private var pushedSparkPredicates = Array.empty[Predicate]

  logger.info(
    s"Constructed DeltaScanBuilder for $tablePath at read " +
      s"version ${snapshot.getVersion()}")

  /**
   * Data sources can implement this interface to push down required columns to the data source
   * and only read these columns during scan to reduce the size of the data to be read.
   *
   * Applies column pruning w.r.t. the given requiredSchema.
   */
  override def pruneColumns(requiredSchema: StructType): Unit = {
    // TODO: verify that requiredSchema is a subset of the table schema
    logger.info(s"Pruning columns for required schema: $requiredSchema")

    // TODO: State machine API doesn't support schema pruning yet
    // For now, just store the required schema for use in DeltaScan
    sparkSchema = requiredSchema
  }

  /**
   * Data sources can implement this interface to push down V2 Predicate to the data source and
   * reduce the size of the data to be read.
   *
   * Pushes down predicates, and returns predicates that need to be evaluated after scanning. Rows
   * should be returned from the data source if and only if all of the predicates match. That is,
   * predicates must be interpreted as ANDed together.
   */
  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    logger.info(s"pushPredicates(): predicates=${predicates.mkString("Array(", ", ", ")")}")
    predicates.foreach(p => logger.info(s"predicate $p, name: ${p.name()}"))

    val sparkToKernelPredicates =
      predicates.map(p => p -> ExpressionUtils.convertStoKPredicate(p)).collect {
        case (sparkPredicate, Some(kernelPredicate)) => sparkPredicate -> kernelPredicate
      }

    logger.info(s"input predicates size: ${predicates.size}")
    logger.info(s"converted predicates size: ${sparkToKernelPredicates.size}")

    if (predicates.length != sparkToKernelPredicates.length) {
      logger.warn("Some predicates could not be converted to kernel predicates")
    }

    logger.info(s"sparkToKernelPredicatesMap: ${sparkToKernelPredicates.mkString(", ")}")

    val kernelAndOpt = sparkToKernelPredicates
      .map(_._2)
      .reduceOption((left, right) => new KernelAnd(left, right))

    logger.info(s"Pushing down predicates: $kernelAndOpt")

    if (kernelAndOpt.nonEmpty) {
      kernelPredicate = kernelAndOpt
      
      // For now, assume all predicates are pushed (partition pruning + data skipping)
      // The state machine will handle this internally
      sparkToKernelPredicates.foreach { case (sparkPred, _) =>
        pushedSparkPredicates = pushedSparkPredicates :+ sparkPred
      }
      
      // Return empty array - all predicates are pushed
      Array.empty
    } else {
      logger.info("No pushable predicates found")
      predicates
    }
  }

  /**
   * Returns the predicates that are pushed to the data source via [[pushPredicates]].
   *
   * There are 3 kinds of predicates:
   *   - pushable predicates which don't need to be evaluated again after scanning.
   *   - pushable predicates which still need to be evaluated after scanning, e.g. parquet row
   *     group predicate.
   *   - non-pushable predicates.
   *
   * Both case 1 and 2 should be considered as pushed predicates and should be returned by this
   * method.
   *
   * It's possible that there is no predicates in the query and [[pushPredicates]] is never
   * called, empty array should be returned for this case.
   */
  override def pushedPredicates(): Array[Predicate] = {
    logger.info(s"pushedPredicates(): ${pushedSparkPredicates.mkString("Array(", ", ", ")")}")
    pushedSparkPredicates
  }

  override def build(): Scan = {
    println(s"[STATE MACHINE] DeltaScanBuilder.build() called - using NEW state machine APIs")
    println(s"[STATE MACHINE] Table path: $tablePath")
    println(s"[STATE MACHINE] Predicate: ${kernelPredicate.map(_.toString).getOrElse("None")}")
    
    // Build scan using state machine API
    val scanBuilder = snapshot.scanBuilder()
    println(s"[STATE MACHINE] Created ScanBuilder from snapshot")
    
    // Add predicate if present
    val scan = if (kernelPredicate.isDefined) {
      println(s"[STATE MACHINE] Adding predicate to scan")
      scanBuilder.withPredicate(kernelPredicate.get).build()
    } else {
      println(s"[STATE MACHINE] Building scan without predicate")
      scanBuilder.build()
    }
    
    println(s"[STATE MACHINE] Scan built successfully, returning DeltaScan")
    new DeltaScan(scan, snapshot, executor, sparkSchema)
  }
}

object DeltaScanBuilder {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}
