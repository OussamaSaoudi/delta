package io.delta

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

import java.util.UUID

class DMLSuite extends QueryTest with SharedSparkSession {
  import DMLSuite._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      .set("spark.sql.catalog.my_delta_catalog", "io.delta.DeltaCatalog")
      // <table_path>/test%dv%prefix-deletion_vector_e47cb0fa-7f45-47d9-bbb9-ab1f15db41d1.bin
      //
      // {"add":{"path":"test%25file%25prefix-part-00001-ac7797e7-efdc-4a36-99a0-7d0b262def4d-c000.
      // snappy.parquet","partitionValues":{},"size":818,"modificationTime":1732217739309,"dataChang
      // e":true,"stats":"{\"numRecords\":5,\"minValues\":{\"id\":5,\"value\":\"test_value\"},\"maxV
      // alues\":{\"id\":9,\"value\":\"test_value\"},\"nullCount\":{\"id\":0,\"value\":0},\"tightBou
      // nds\":false}","deletionVector":{"storageType":"u","pathOrInlineDv":"<B1idE)((XYsB%:72844","
      // offset":1,"sizeInBytes":36,"cardinality":2}}}
      //
      // Caused by: java.io.FileNotFoundException: File file:/tmp/spark_warehouse/table_6696d2fc/de
      // letion_vector_e47cb0fa-7f45-47d9-bbb9-ab1f15db41d1.bin does not exist
      .set("spark.databricks.delta.testOnly.dataFileNamePrefix", "")
      .set("spark.databricks.delta.testOnly.dvFileNamePrefix", "")
  }

  def withUniqueTableIdAndItsPath(test: (String, String) => Unit): Unit = {
    val tableName = s"table_${UUID.randomUUID().toString.substring(0, 8)}"
    val tid = s"my_delta_catalog.$tableName"
    val path = s"/tmp/spark_warehouse/$tableName"
//    println(s"Using table id: $tid")
//    println(s"Using table path: $path")
    test(tid, path)
  }














  test("read a delta table with rust dsv2") {
    println("RUST")
    val id = "my_delta_catalog.benchmark_table_oxidized_java"
    val readDataDSv2 =
      spark.read
        .format("delta2") // Use delta spark connector v2
        .table(id)
    readDataDSv2.show(5)
  }













  test("aaa") {
    withUniqueTableIdAndItsPath { (tableId, path) =>
      logger.info("Scott > STEP 1a: create table with dsv1")


      val id2 = "my_delta_catalog.benchmark_table_oxidized_java"
      val path2 = "/tmp/spark_warehouse/benchmark_table_oxidized_java"
      val condition = "true"
      // tests_added > 5 AND repository = 'analytics' AND NOT (committer_age IS NULL) AND  NOT (region = 'Australia')
//      println("DSV1")
//      val postMergeDSv1 = spark.time(
//        spark.read
//          .format("delta")
//          .load(path2)
//          .where(condition)
//          )
////      print(postMergeDSv1.inputFiles.mkString("Array(", ", ", ")"))
////      postMergeDSv1.show(5)
//
      println("RUST")
      val readDataDSv2 = spark.read
          .format("delta2")
          .table(id2)
//          .where(condition)
      readDataDSv2.show(5)
    }
  }

  test("bbb") {
    withUniqueTableIdAndItsPath { (tid, path) =>
      spark
        .range(10)
        .withColumn("part1", col("id") % 5)
        .withColumn("col1", col("id").cast("long"))
        .withColumn("col2", concat(lit("value_"), col("id").cast("string")))
        .withColumn("col3", col("id") % 2 === 0)
        .drop("id")
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("part1")
        .option("delta.enableDeletionVectors", "true")
        .save(path)

      logger.info(s"Scott > Table $tid created")

      spark.table(tid).show(100)

      logger.info("Scott > performing delete")

      spark.sql(s"DELETE FROM $tid WHERE col1 < 3")

      spark.table(tid).show(100)
    }
  }
}

object DMLSuite {
  val logger = org.slf4j.LoggerFactory.getLogger("DMLSuite")
}
