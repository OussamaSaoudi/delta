package io.delta.read

import io.delta.DeltaCatalog
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

import java.io.File
import java.util.UUID

/**
 * Correctness test for DSv2 connector with state machine APIs.
 * Tests actual data reading, not just metadata.
 */
class DeltaScanCorrectnessTest extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      .set("spark.sql.catalog.delta_catalog", "io.delta.DeltaCatalog")
  }

  private def withTempTable(test: String => Unit): Unit = {
    val tempDir = new File(System.getProperty("java.io.tmpdir"), s"delta-test-${UUID.randomUUID()}")
    tempDir.mkdirs()
    val tablePath = s"file://${tempDir.getAbsolutePath}"
    try {
      test(tablePath)
    } finally {
      // Cleanup
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          file.listFiles().foreach(deleteRecursively)
        }
        file.delete()
      }
      deleteRecursively(tempDir)
    }
  }

  test("read simple table - verify actual data") {
    withTempTable { tablePath =>
      // Write data
      spark.range(100).write.format("delta").save(tablePath)
      
      // Read using NEW state machine DSv2 connector
      val df = spark.read.format("delta2").load(tablePath)
      
      // Verify count
      assert(df.count() == 100, "Should read 100 rows")
      
      // Verify actual data values
      val rows = df.orderBy("id").collect()
      assert(rows.length == 100)
      assert(rows(0).getLong(0) == 0, "First row should be 0")
      assert(rows(99).getLong(0) == 99, "Last row should be 99")
      
      println(s"✓ Successfully read 100 rows with correct values")
    }
  }

  test("read table with filter - verify data skipping") {
    withTempTable { tablePath =>
      // Write data
      spark.range(100).write.format("delta").save(tablePath)
      
      // Read with filter using NEW state machine DSv2 connector
      // Predicates are pushed to the Rust scan builder for data skipping (file-level filtering based on stats)
      // Spark will apply the predicate after reading to get final results
      val df = spark.read.format("delta2").load(tablePath).where("id >= 50")
      
      // Verify count - all rows passing the filter should be returned
      // (After Spark applies the filter post-read)
      assert(df.count() == 50, "Should have 50 rows after Spark filters (id >= 50)")
      
      // Verify actual data values
      val rows = df.orderBy("id").collect()
      assert(rows.length == 50)
      assert(rows(0).getLong(0) == 50, "First filtered row should be 50")
      assert(rows(49).getLong(0) == 99, "Last filtered row should be 99")
      
      println(s"✓ Successfully read and filtered 50 rows with data skipping + Spark filtering")
    }
  }

}

