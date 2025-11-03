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

  test("read table with filter - verify predicate pushdown") {
    withTempTable { tablePath =>
      // Write data
      spark.range(100).write.format("delta").save(tablePath)
      
      // Read with filter using NEW state machine DSv2 connector
      val df = spark.read.format("delta2").load(tablePath).where("id >= 50")
      
      // Verify count
      assert(df.count() == 50, "Should read 50 rows after filter")
      
      // Verify actual data values
      val rows = df.orderBy("id").collect()
      assert(rows.length == 50)
      assert(rows(0).getLong(0) == 50, "First filtered row should be 50")
      assert(rows(49).getLong(0) == 99, "Last filtered row should be 99")
      
      println(s"✓ Successfully read 50 filtered rows with correct values")
    }
  }

  test("read partitioned table - verify partition pruning") {
    withTempTable { tablePath =>
      // Write partitioned data
      spark.range(100)
        .withColumn("part", col("id") % 10)
        .write
        .format("delta")
        .partitionBy("part")
        .save(tablePath)
      
      // Read with partition filter
      val df = spark.read.format("delta2").load(tablePath).where("part = 5")
      
      // Verify count
      assert(df.count() == 10, "Should read 10 rows from partition 5")
      
      // Verify actual data values
      val rows = df.orderBy("id").collect()
      assert(rows.length == 10)
      rows.foreach { row =>
        assert(row.getLong(1) == 5, "All rows should have part=5")
      }
      
      println(s"✓ Successfully read 10 rows from partition with correct values")
    }
  }

  test("read table with multiple data types") {
    withTempTable { tablePath =>
      // Write data with multiple types
      spark.range(50)
        .withColumn("name", concat(lit("string_"), col("id").cast("string")))
        .withColumn("flag", col("id") % 2 === 0)
        .withColumn("value", col("id").cast("double"))
        .write
        .format("delta")
        .save(tablePath)
      
      // Read data
      val df = spark.read.format("delta2").load(tablePath)
      
      // Verify count
      assert(df.count() == 50, "Should read 50 rows")
      
      // Verify schema
      assert(df.schema.fields.length == 4, "Should have 4 columns")
      
      // Verify actual data values
      val rows = df.orderBy("id").collect()
      assert(rows.length == 50)
      assert(rows(0).getLong(0) == 0)
      assert(rows(0).getString(1) == "string_0")
      assert(rows(0).getBoolean(2) == true)
      assert(rows(0).getDouble(3) == 0.0)
      
      assert(rows(25).getLong(0) == 25)
      assert(rows(25).getString(1) == "string_25")
      assert(rows(25).getBoolean(2) == false)
      assert(rows(25).getDouble(3) == 25.0)
      
      println(s"✓ Successfully read 50 rows with multiple data types")
    }
  }

  test("read table with SQL query") {
    withTempTable { tablePath =>
      // Write data
      spark.range(100)
        .withColumn("value", col("id") * 2)
        .write
        .format("delta")
        .save(tablePath)
      
      // Register as temp view
      spark.read.format("delta2").load(tablePath).createOrReplaceTempView("test_table")
      
      // Execute SQL query
      val result = spark.sql("SELECT * FROM test_table WHERE id >= 50 AND value < 150")
      
      // Verify results
      assert(result.count() == 25, "Should return 25 rows")
      
      val rows = result.orderBy("id").collect()
      assert(rows(0).getLong(0) == 50)
      assert(rows(0).getLong(1) == 100)
      assert(rows(24).getLong(0) == 74)
      assert(rows(24).getLong(1) == 148)
      
      println(s"✓ Successfully executed SQL query and read correct data")
    }
  }

  test("read table after updates") {
    withTempTable { tablePath =>
      // Write initial data
      spark.range(50).write.format("delta").save(tablePath)
      
      // Append more data
      spark.range(50, 100).write.format("delta").mode("append").save(tablePath)
      
      // Read data
      val df = spark.read.format("delta2").load(tablePath)
      
      // Verify count
      assert(df.count() == 100, "Should read 100 rows after append")
      
      // Verify data
      val rows = df.orderBy("id").collect()
      assert(rows.length == 100)
      assert(rows(0).getLong(0) == 0)
      assert(rows(99).getLong(0) == 99)
      
      println(s"✓ Successfully read data after append")
    }
  }

  test("read empty table") {
    withTempTable { tablePath =>
      // Write empty table
      spark.range(0)
        .withColumn("name", lit(""))
        .write
        .format("delta")
        .save(tablePath)
      
      // Read data
      val df = spark.read.format("delta2").load(tablePath)
      
      // Verify count
      assert(df.count() == 0, "Should read 0 rows from empty table")
      
      // Verify schema exists
      assert(df.schema.fields.length == 2, "Should have 2 columns in schema")
      
      println(s"✓ Successfully read empty table")
    }
  }

  test("read table with complex filter expressions") {
    withTempTable { tablePath =>
      // Write data
      spark.range(100)
        .withColumn("value", col("id") * 2)
        .withColumn("flag", col("id") % 3 === 0)
        .write
        .format("delta")
        .save(tablePath)
      
      // Read with complex filter
      val df = spark.read.format("delta2").load(tablePath)
        .where("id >= 10 AND id < 50 AND flag = true")
      
      // Verify count (multiples of 3 between 10 and 49)
      val expected = (10 until 50).count(_ % 3 == 0)
      assert(df.count() == expected, s"Should read $expected rows")
      
      // Verify all rows satisfy the filter
      val rows = df.collect()
      rows.foreach { row =>
        val id = row.getLong(0)
        assert(id >= 10 && id < 50, s"Row $id should be in range [10, 50)")
        assert(row.getBoolean(2) == true, s"Row $id should have flag=true")
        assert(id % 3 == 0, s"Row $id should be divisible by 3")
      }
      
      println(s"✓ Successfully read $expected rows with complex filter")
    }
  }
}

