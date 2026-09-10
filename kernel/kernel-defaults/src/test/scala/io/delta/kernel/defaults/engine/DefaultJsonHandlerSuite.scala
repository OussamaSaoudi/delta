/*
 * Copyright (2021) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.engine

import java.math.{BigDecimal => JBigDecimal}
import java.nio.file.FileAlreadyExistsException
import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.data.ColumnVector
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO
import io.delta.kernel.defaults.internal.data.{DefaultColumnarBatch, DefaultRowBasedColumnarBatch}
import io.delta.kernel.defaults.internal.data.vector.{DefaultConstantVector, DefaultStructVector}
import io.delta.kernel.defaults.utils.{DefaultVectorTestUtils, TestRow, TestUtils}
import io.delta.kernel.internal.actions.CommitInfo
import io.delta.kernel.internal.util.InternalUtils.singletonStringColumnVector
import io.delta.kernel.types._

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class DefaultJsonHandlerSuite extends AnyFunSuite with TestUtils with DefaultVectorTestUtils {

  val jsonHandler = new DefaultJsonHandler(
    new HadoopFileIO(
      new Configuration {
        set("delta.kernel.default.json.reader.batch-size", "1")
      }))
  val fsClient = defaultEngine.getFileSystemClient

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Tests for parseJson for statistics eligible types (additional in TestDefaultJsonHandler.java)
  /////////////////////////////////////////////////////////////////////////////////////////////////

  def testJsonParserWithSchema(
      jsonString: String,
      schema: StructType,
      expectedRow: TestRow): Unit = {
    val batchRows = jsonHandler.parseJson(
      singletonStringColumnVector(jsonString),
      schema,
      Optional.empty()).getRows.toSeq
    checkAnswer(batchRows, Seq(expectedRow))
  }

  def testJsonParserForSingleType(
      jsonString: String,
      dataType: DataType,
      numColumns: Int,
      expectedRow: TestRow): Unit = {
    val schema = new StructType(
      (1 to numColumns).map(i => new StructField(s"col$i", dataType, true)).asJava)
    testJsonParserWithSchema(jsonString, schema, expectedRow)
  }

  def testOutOfRangeValue(stringValue: String, dataType: DataType): Unit = {
    val e = intercept[RuntimeException] {
      testJsonParserForSingleType(
        jsonString = s"""{"col1":$stringValue}""",
        dataType = dataType,
        numColumns = 1,
        expectedRow = TestRow())
    }
    assert(e.getMessage.contains(s"Couldn't decode $stringValue"))
  }

  test("parse byte type") {
    testJsonParserForSingleType(
      jsonString = """{"col1":0,"col2":-127,"col3":127,"col4":null,"col5":1.0}""",
      dataType = ByteType.BYTE,
      5,
      TestRow(0.toByte, -127.toByte, 127.toByte, null, 1.toByte))
    testOutOfRangeValue("128", ByteType.BYTE)
    testOutOfRangeValue("-129", ByteType.BYTE)
    testOutOfRangeValue("2147483648", ByteType.BYTE)
    testOutOfRangeValue("1.5", ByteType.BYTE)
    testOutOfRangeValue("128.0", ByteType.BYTE)
  }

  test("parse short type") {
    testJsonParserForSingleType(
      jsonString = """{"col1":-32767,"col2":8,"col3":32767,"col4":null,"col5":1e3}""",
      dataType = ShortType.SHORT,
      5,
      TestRow(-32767.toShort, 8.toShort, 32767.toShort, null, 1000.toShort))
    testOutOfRangeValue("32768", ShortType.SHORT)
    testOutOfRangeValue("-32769", ShortType.SHORT)
    testOutOfRangeValue("2147483648", ShortType.SHORT)
    testOutOfRangeValue("1.5", ShortType.SHORT)
    testOutOfRangeValue("32768.0", ShortType.SHORT)
  }

  test("parse integer type") {
    testJsonParserForSingleType(
      jsonString = """{"col1":-2147483648,"col2":8,"col3":2147483647, "col4":null}""",
      dataType = IntegerType.INTEGER,
      4,
      TestRow(-2147483648, 8, 2147483647, null))
    testOutOfRangeValue("2147483648", IntegerType.INTEGER)
    testOutOfRangeValue("-2147483649", IntegerType.INTEGER)
    testOutOfRangeValue("1.0", IntegerType.INTEGER)
  }

  test("parse long type") {
    testJsonParserForSingleType(
      jsonString =
        """{"col1":-9223372036854775808,"col2":8,"col3":9223372036854775807, "col4":null}""",
      dataType = LongType.LONG,
      4,
      TestRow(-9223372036854775808L, 8L, 9223372036854775807L, null))
    testOutOfRangeValue("9223372036854775808", LongType.LONG)
    testOutOfRangeValue("-9223372036854775809", LongType.LONG)
    testOutOfRangeValue("1.0", LongType.LONG)
  }

  test("parse float type") {
    testJsonParserForSingleType(
      jsonString =
        """
          |{"col1":-9223.33,"col2":0.4,"col3":1.2E8,
          |"col4":1.23E-7,"col5":0.004444444,"col6":null,"col7":-0.0}""".stripMargin,
      dataType = FloatType.FLOAT,
      7,
      TestRow(-9223.33f, 0.4f, 120000000.0f, 0.000000123f, 0.004444444f, null, 0.0f))
    testOutOfRangeValue("3.4028235E+39", FloatType.FLOAT)
  }

  test("parse double type") {
    testJsonParserForSingleType(
      jsonString =
        """
          |{"col1":-9.2233333333E8,"col2":0.4,"col3":1.2E8,
          |"col4":1.234444444E-7,"col5":0.0444444444,"col6":null,"col7":-0.0}""".stripMargin,
      dataType = DoubleType.DOUBLE,
      7,
      TestRow(
        -922333333.33d,
        0.4d,
        120000000.0d,
        0.0000001234444444d,
        0.0444444444d,
        null,
        0.0d))
    // For some reason out-of-range doubles are parsed initially as Double.INFINITY instead of
    // a BigDecimal
    val e = intercept[RuntimeException] {
      testJsonParserForSingleType(
        jsonString = s"""{"col1":1.7976931348623157E+309}""",
        dataType = DoubleType.DOUBLE,
        numColumns = 1,
        expectedRow = TestRow())
    }
    assert(e.getMessage.contains(s"Couldn't decode"))
  }

  test("parse string type") {
    testJsonParserForSingleType(
      jsonString = """{"col1": "foo", "col2": "", "col3": null}""",
      dataType = StringType.STRING,
      3,
      TestRow("foo", "", null))
  }

  test("parse decimal type") {
    testJsonParserWithSchema(
      jsonString = """
      |{
      |  "col1":0,
      |  "col2":0.01234567891234567891234567891234567890,
      |  "col3":123456789123456789123456789123456789,
      |  "col4":1234567891234567891234567891.2345678900,
      |  "col5":1.23,
      |  "col6":null,
      |  "col7":1.2300,
      |  "col8":-0.00
      |}
      |""".stripMargin,
      schema = new StructType()
        .add("col1", DecimalType.USER_DEFAULT)
        .add("col2", new DecimalType(38, 38))
        .add("col3", new DecimalType(38, 0))
        .add("col4", new DecimalType(38, 10))
        .add("col5", new DecimalType(5, 2))
        .add("col6", new DecimalType(5, 2))
        .add("col7", new DecimalType(5, 4))
        .add("col8", new DecimalType(5, 2)),
      TestRow(
        new JBigDecimal(0),
        new JBigDecimal("0.01234567891234567891234567891234567890"),
        new JBigDecimal("123456789123456789123456789123456789"),
        new JBigDecimal("1234567891234567891234567891.2345678900"),
        new JBigDecimal("1.23"),
        null,
        new JBigDecimal("1.23"),
        JBigDecimal.ZERO))
  }

  test("parse date type") {
    testJsonParserForSingleType(
      jsonString = """{"col1":"2020-12-31", "col2":"1965-01-31", "col3": null}""",
      dataType = DateType.DATE,
      3,
      TestRow(18627, -1796, null))
  }

  test("parse timestamp type") {
    testJsonParserForSingleType(
      jsonString =
        """
          |{
          | "col1":"2050-01-01T00:00:00.000-08:00",
          | "col2":"1970-01-01T06:30:23.523Z",
          | "col3":"1960-01-01T10:00:00.000Z",
          | "col4":null
          | }
          | """.stripMargin,
      dataType = TimestampType.TIMESTAMP,
      numColumns = 4,
      TestRow(2524636800000000L, 23423523000L, -315583200000000L, null))
  }

  test("parse timestamp type with large values") {
    // Timestamps far in the future should not cause overflow.
    // ChronoUnit.MICROS.between() internally computes nanoseconds first, which overflows
    // for timestamps more than ~292 years from epoch.
    testJsonParserForSingleType(
      jsonString = """{"col1":"9999-12-31T23:59:59.000+00:00"}""",
      dataType = TimestampType.TIMESTAMP,
      numColumns = 1,
      TestRow(253402300799000000L))
  }

  test("parse null input") {
    val schema = new StructType()
      .add("nested_struct", new StructType().add("foo", IntegerType.INTEGER))

    val batch = jsonHandler.parseJson(
      singletonStringColumnVector(null),
      schema,
      Optional.empty())
    assert(batch.isInstanceOf[DefaultColumnarBatch])
    assert(batch.getColumnVector(0).isInstanceOf[DefaultStructVector])
    assert(batch.getColumnVector(0).getChild(0).isNullAt(0))
  }

  test("parse NaN and INF for float and double") {
    def testSpecifiedString(json: String, output: TestRow): Unit = {
      testJsonParserWithSchema(
        jsonString = json,
        schema = new StructType()
          .add("col1", FloatType.FLOAT)
          .add("col2", DoubleType.DOUBLE),
        output)
    }
    testSpecifiedString("""{"col1":"NaN","col2":"NaN"}""", TestRow(Float.NaN, Double.NaN))
    testSpecifiedString(
      """{"col1":"+INF","col2":"+INF"}""",
      TestRow(Float.PositiveInfinity, Double.PositiveInfinity))
    testSpecifiedString(
      """{"col1":"+Infinity","col2":"+Infinity"}""",
      TestRow(Float.PositiveInfinity, Double.PositiveInfinity))
    testSpecifiedString(
      """{"col1":"Infinity","col2":"Infinity"}""",
      TestRow(Float.PositiveInfinity, Double.PositiveInfinity))
    testSpecifiedString(
      """{"col1":"-INF","col2":"-INF"}""",
      TestRow(Float.NegativeInfinity, Double.NegativeInfinity))
    testSpecifiedString(
      """{"col1":"-Infinity","col2":"-Infinity"}""",
      TestRow(Float.NegativeInfinity, Double.NegativeInfinity))
  }

  test("don't parse unselected rows") {
    val selectionVector = booleanVector(Seq(true, false, false))
    val jsonVector = stringVector(
      Seq("""{"col1":1}""", """{"col1":"foo"}""", """{"col1":"foo"}"""))
    val batch = jsonHandler.parseJson(
      jsonVector,
      new StructType()
        .add("col1", IntegerType.INTEGER),
      Optional.of(selectionVector))
    assert(batch.isInstanceOf[DefaultColumnarBatch])
    val rows = batch.getRows.toSeq
    assert(!rows(0).isNullAt(0) && rows(0).getInt(0) == 1)
    assert(rows(1).isNullAt(0) && rows(2).isNullAt(0))
  }

  test("read json files") {
    val expResults = Seq(
      TestRow("part-00000-d83dafd8-c344-49f0-ab1c-acd944e32493-c000.snappy.parquet", 348L, true),
      TestRow("part-00000-cb078bc1-0aeb-46ed-9cf8-74a843b32c8c-c000.snappy.parquet", 687L, true),
      TestRow("part-00001-9bf4b8f8-1b95-411b-bf10-28dc03aa9d2f-c000.snappy.parquet", 705L, true),
      TestRow("part-00000-0441e99a-c421-400e-83a1-212aa6c84c73-c000.snappy.parquet", 650L, true),
      TestRow("part-00001-34c8c673-3f44-4fa7-b94e-07357ec28a7d-c000.snappy.parquet", 650L, true),
      TestRow("part-00000-842017c2-3e02-44b5-a3d6-5b9ae1745045-c000.snappy.parquet", 649L, true),
      TestRow("part-00001-e62ca5a1-923c-4ee6-998b-c61d1cfb0b1c-c000.snappy.parquet", 649L, true))
    Seq(
      (
        fsClient.listFrom(getTestResourceFilePath("json-files/1.json")),
        expResults),
      (
        fsClient.listFrom(getTestResourceFilePath("json-files-with-empty/1.json")),
        expResults),
      (
        fsClient.listFrom(getTestResourceFilePath("json-files-with-empty/5.json")),
        expResults.takeRight(2)),
      (
        fsClient.listFrom(getTestResourceFilePath("json-files-all-empty/1.json")),
        Seq())).foreach {
      case (testFiles, expResults) =>
        val batches = jsonHandler.readJsonFiles(
          testFiles,
          new StructType()
            .add("path", StringType.STRING)
            .add("size", LongType.LONG)
            .add("dataChange", BooleanType.BOOLEAN),
          Optional.empty()).toSeq
        assert(batches.forall(_.isInstanceOf[DefaultColumnarBatch]))
        val actResult = batches.map(batch => TestRow(batch.getRows.next))

        checkAnswer(actResult, expResults)
    }
  }

  test("parse json content") {
    val input = """
      |{
      |  "path":"part-00000-d83dafd8-c344-49f0-ab1c-acd944e32493-c000.snappy.parquet",
      |  "partitionValues":{"p1" : "0", "p2" : "str"},
      |  "size":348,
      |  "modificationTime":1603723974000,
      |  "dataChange":true
      |}
      |""".stripMargin
    val readSchema = new StructType()
      .add("path", StringType.STRING)
      .add("partitionValues", new MapType(StringType.STRING, StringType.STRING, false))
      .add("size", LongType.LONG)
      .add("dataChange", BooleanType.BOOLEAN)

    val batch = jsonHandler.parseJson(
      singletonStringColumnVector(input),
      readSchema,
      Optional.empty[ColumnVector]())
    assert(batch.getSize == 1)

    val actResult = Seq(TestRow(batch.getRows.next))
    val expResult = Seq(TestRow(
      "part-00000-d83dafd8-c344-49f0-ab1c-acd944e32493-c000.snappy.parquet",
      Map("p1" -> "0", "p2" -> "str"),
      348L,
      true))

    checkAnswer(actResult, expResult)
  }

  test("parse nested complex types") {
    val json = """
      |{
      |  "array": [0, 1, null],
      |  "nested_array": [["a", "b"], ["c"], []],
      |  "map": {"a":  true, "b":  false},
      |  "nested_map": {"a":  {"one":  [], "two":  [1, 2, 3]}, "b":  {}},
      |  "array_of_struct": [{"field1": "foo", "field2": 3}, {"field1": null}]
      |}
      |""".stripMargin

    val schema = new StructType()
      .add("array", new ArrayType(IntegerType.INTEGER, true))
      .add("nested_array", new ArrayType(new ArrayType(StringType.STRING, true), true))
      .add("map", new MapType(StringType.STRING, BooleanType.BOOLEAN, true))
      .add(
        "nested_map",
        new MapType(
          StringType.STRING,
          new MapType(StringType.STRING, new ArrayType(IntegerType.INTEGER, true), true),
          true))
      .add(
        "array_of_struct",
        new ArrayType(
          new StructType()
            .add("field1", StringType.STRING, true)
            .add("field2", IntegerType.INTEGER, true),
          true))
    val batch = jsonHandler.parseJson(
      singletonStringColumnVector(json),
      schema,
      Optional.empty[ColumnVector]())

    val actResult = Seq(TestRow(batch.getRows.next))
    val expResult = Seq(TestRow(
      Vector(0, 1, null),
      Vector(Vector("a", "b"), Vector("c"), Vector()),
      Map("a" -> true, "b" -> false),
      Map(
        "a" -> Map(
          "one" -> Vector(),
          "two" -> Vector(1, 2, 3)),
        "b" -> Map()),
      Vector(TestRow.fromSeq(Seq("foo", 3)), TestRow.fromSeq(Seq(null, null)))))

    checkAnswer(actResult, expResult)
  }

  test("streaming parser skips unknown values and preserves field order independence") {
    val schema = new StructType()
      .add("number", IntegerType.INTEGER)
      .add("nested", new StructType().add("text", StringType.STRING))

    testJsonParserWithSchema(
      """{
        |  "unknown": {"deep": [1, {"ignored": true}]},
        |  "nested": {"anotherUnknown": [false], "text": "value"},
        |  "number": 10
        |}""".stripMargin,
      schema,
      TestRow(10, TestRow("value")))
  }

  test("streaming parser applies last-value-wins to duplicate fields and map keys") {
    val schema = new StructType()
      .add("number", IntegerType.INTEGER, false)
      .add("nested", new StructType().add("value", LongType.LONG, false), false)
      .add("map", new MapType(StringType.STRING, IntegerType.INTEGER, false), false)

    testJsonParserWithSchema(
      """{
        |  "number": "invalid", "number": 5,
        |  "nested": {"value": "invalid"}, "nested": {"value": 6},
        |  "map": {"key": "invalid", "key": 7}
        |}""".stripMargin,
      schema,
      TestRow(5, TestRow(6L), Map("key" -> 7)))
  }

  test("columnar parser lazily populates primitive columns") {
    val schema = new StructType().add("value", ByteType.BYTE)
    val batch = jsonHandler.parseJson(
      stringVector(Seq(
        """{"value":null,"value":5}""",
        """{"value":6,"value":null}""",
        "{}",
        """{"value":7}""")),
      schema,
      Optional.empty())

    val values = batch.getColumnVector(0)
    assert(!values.isNullAt(0) && values.getByte(0) == 5.toByte)
    assert(values.isNullAt(1))
    assert(values.isNullAt(2))
    assert(!values.isNullAt(3) && values.getByte(3) == 7.toByte)
  }

  test("columnar parser represents an unpopulated leaf as an all-null constant") {
    val schema = new StructType().add("value", LongType.LONG)
    val batch = jsonHandler.parseJson(
      stringVector(Seq("{}", """{"value":null}""")),
      schema,
      Optional.empty())

    val values = batch.getColumnVector(0)
    assert(values.isInstanceOf[DefaultConstantVector])
    assert(values.getDataType == LongType.LONG)
    assert(values.getSize == 2)
    assert(values.isNullAt(0) && values.isNullAt(1))
  }

  test("columnar parser clears children when the last duplicate struct is null") {
    val schema = new StructType()
      .add("nested", new StructType().add("value", IntegerType.INTEGER))
    val batch = jsonHandler.parseJson(
      singletonStringColumnVector("""{"nested":{"value":1},"nested":null}"""),
      schema,
      Optional.empty())
    val nested = batch.getColumnVector(0)
    assert(nested.isNullAt(0))
    assert(nested.getChild(0).isNullAt(0))
  }

  test("duplicate schema names use the strict tree fallback for every ordinal") {
    val schema = new StructType()
      .add("value", ByteType.BYTE, false)
      .add("value", DecimalType.USER_DEFAULT, false)

    val batch = jsonHandler.parseJson(
      singletonStringColumnVector("""{"value": 1.0}"""),
      schema,
      Optional.empty())
    assert(batch.isInstanceOf[DefaultRowBasedColumnarBatch])
    checkAnswer(batch.getRows.toSeq, Seq(TestRow(1.toByte, JBigDecimal.ONE)))
  }

  test("streaming parser retains strict nullability semantics") {
    val schema = new StructType()
      .add("required", IntegerType.INTEGER, false)
      .add("optional", StringType.STRING, true)

    testJsonParserWithSchema(
      """{"required": 1}""",
      schema,
      TestRow(1, null))

    Seq("{}", """{"required": null}""").foreach {
      json =>
        val error = intercept[RuntimeException] {
          jsonHandler.parseJson(singletonStringColumnVector(json), schema, Optional.empty())
        }
        assert(error.getMessage.contains("required"))
    }
  }

  test("streaming parser preserves strict decimal and timestamp coercion") {
    val schema = new StructType()
      .add("decimal", new DecimalType(20, 4))
      .add("timestamp", TimestampType.TIMESTAMP)
      .add("timestampNtz", TimestampNTZType.TIMESTAMP_NTZ)

    testJsonParserWithSchema(
      """{
        |  "decimal": 1234567890.125,
        |  "timestamp": "1970-01-01T00:00:00Z",
        |  "timestampNtz": "1970-01-01T00:00:00"
        |}""".stripMargin,
      schema,
      TestRow(new JBigDecimal("1234567890.125"), 0L, 0L))

    Seq(
      """{"decimal": "1.25", "timestamp": "1970-01-01T00:00:00Z",
        |"timestampNtz": "1970-01-01T00:00:00"}""".stripMargin,
      """{"decimal": 1.25, "timestamp": 0,
        |"timestampNtz": "1970-01-01T00:00:00"}""".stripMargin).foreach { json =>
      intercept[RuntimeException] {
        jsonHandler.parseJson(singletonStringColumnVector(json), schema, Optional.empty())
      }
    }
  }

  test("strict parser keeps the first object when trailing JSON values are present") {
    val schema = new StructType().add("value", IntegerType.INTEGER)
    val batch = jsonHandler.parseJson(
      singletonStringColumnVector("""{"value": 1} {"value": 2}"""),
      schema,
      Optional.empty())
    assert(batch.isInstanceOf[DefaultColumnarBatch])
    checkAnswer(batch.getRows.toSeq, Seq(TestRow(1)))
  }

  test("write rows as json") {
    withTempDir { tempDir =>
      val input = Seq(
        """{
          | "add":
          |  {
          |    "path":"part-00000-d83dafd8-c344-49f0-ab1c-acd944e32493-c000.snappy.parquet",
          |    "partitionValues":{"p1" : "0", "p2" : "str"},
          |    "size":348,
          |    "dataChange":true
          |  }
          |}
          |""".stripMargin.linesIterator.mkString,
        """{
          | "remove":
          |  {
          |    "path":"part-00000-d83dafd8-c344-49f0-ab1c-acd944e32493-c000.snappy.parquet",
          |    "partitionValues":{"p1" : "0", "p2" : "str"},
          |    "size":348,
          |    "dataChange":true
          |  }
          |}
          |""".stripMargin.linesIterator.mkString)

      val addRemoveSchema = new StructType()
        .add("path", StringType.STRING)
        .add("partitionValues", new MapType(StringType.STRING, StringType.STRING, false))
        .add("size", LongType.LONG)
        .add("dataChange", BooleanType.BOOLEAN)

      val readSchema = new StructType()
        .add("add", addRemoveSchema)
        .add("remove", addRemoveSchema)

      val batch = jsonHandler.parseJson(stringVector(input), readSchema, Optional.empty())
      assert(batch.getSize == 2)

      val filePath = tempDir + "/1.json"
      def writeAndVerify(overwrite: Boolean): Unit = {
        jsonHandler.writeJsonFileAtomically(filePath, batch.getRows, overwrite)

        // read it back and verify the contents are correct
        val source = scala.io.Source.fromFile(filePath)
        val result =
          try source.getLines().mkString(",")
          finally source.close()

        // remove the whitespaces from the input to compare
        assert(input.map(_.replaceAll(" ", "")).mkString(",") === result)
      }

      writeAndVerify(overwrite = false)

      // Try to write as same file with overwrite as false and expect an error
      intercept[FileAlreadyExistsException] {
        jsonHandler.writeJsonFileAtomically(filePath, batch.getRows, false /* overwrite */ )
      }

      // Try to write as file with overwrite set to true
      writeAndVerify(overwrite = true)
    }
  }

  test("parse geometry type as WKT string") {
    val schema = new StructType()
      .add("geom", GeometryType.ofDefault())
    testJsonParserWithSchema(
      """{"geom": "POINT (1.0 2.0)"}""",
      schema,
      TestRow("POINT (1.0 2.0)"))
  }

  test("parse geography type as WKT string") {
    val schema = new StructType()
      .add("geog", GeographyType.ofDefault())
    testJsonParserWithSchema(
      """{"geog": "POINT (10.5 20.5)"}""",
      schema,
      TestRow("POINT (10.5 20.5)"))
  }

  test("parse geometry/geography with null values") {
    val schema = new StructType()
      .add("geom", GeometryType.ofDefault(), true)
      .add("geog", GeographyType.ofDefault(), true)
    testJsonParserWithSchema(
      """{"geom": null, "geog": null}""",
      schema,
      TestRow(null, null))
  }

  test("parse geometry POINT variants (Z, M, ZM)") {
    val schema = new StructType()
      .add("col1", GeometryType.ofDefault())
    testJsonParserWithSchema(
      """{"col1": "POINT Z(1.0 2.0 3.0)"}""",
      schema,
      TestRow("POINT Z(1.0 2.0 3.0)"))
    testJsonParserWithSchema(
      """{"col1": "POINT M(1.0 2.0 4.0)"}""",
      schema,
      TestRow("POINT M(1.0 2.0 4.0)"))
    testJsonParserWithSchema(
      """{"col1": "POINT ZM(1.0 2.0 3.0 4.0)"}""",
      schema,
      TestRow("POINT ZM(1.0 2.0 3.0 4.0)"))
  }

  test("parse geometry with non-string value throws") {
    val schema = new StructType()
      .add("geom", GeometryType.ofDefault())
    val e = intercept[RuntimeException] {
      testJsonParserWithSchema(
        """{"geom": 1234}""",
        schema,
        TestRow())
    }
    assert(e.getMessage.contains("string"))
  }

  test("parse diverse type values in a map[string, string]") {
    val input =
      """
        |{
        |   "inCommitTimestamp":1740009523401,
        |   "timestamp":1740009523401,
        |   "engineInfo":"myengine.com",
        |   "operation":"WRITE",
        |   "operationParameters":
        |     {"mode":"Append","statsOnLoad":false,"partitionBy":"[]"},
        |   "isBlindAppend":true,
        |   "txnId":"cb009f42-5da1-4e7e-b4fa-09de3332f52a",
        |   "operationMetrics": {
        |       "numFiles":"1",
        |       "serializedAsNumber":2,
        |       "serializedAsBoolean":true
        |   }
        |}
        |""".stripMargin

    val output = jsonHandler.parseJson(
      stringVector(Seq(input)),
      CommitInfo.FULL_SCHEMA,
      Optional.empty())
    assert(output.getSize == 1)
    val actResult = TestRow(output.getRows.next)
    val expResult = TestRow(
      1740009523401L,
      1740009523401L,
      "myengine.com",
      "WRITE",
      Map("mode" -> "Append", "statsOnLoad" -> "false", "partitionBy" -> "[]"),
      true,
      "cb009f42-5da1-4e7e-b4fa-09de3332f52a",
      Map("numFiles" -> "1", "serializedAsNumber" -> "2", "serializedAsBoolean" -> "true"))

    checkAnswer(Seq(actResult), Seq(expResult))
  }

  test("parse CommitInfo JSON with missing isBlindAppend field") {
    val input =
      """
        |{
        |   "inCommitTimestamp":1740009523401,
        |   "timestamp":1740009523401,
        |   "engineInfo":"myengine.com",
        |   "operation":"WRITE",
        |   "operationParameters":
        |     {"mode":"Append","partitionBy":"[]"},
        |   "txnId":"cb009f42-5da1-4e7e-b4fa-09de3332f52a",
        |   "operationMetrics": {
        |       "numFiles":"1"
        |   }
        |}
        |""".stripMargin

    val output = jsonHandler.parseJson(
      stringVector(Seq(input)),
      CommitInfo.FULL_SCHEMA,
      Optional.empty())
    assert(output.getSize == 1)
    val actResult = TestRow(output.getRows.next)
    val expResult = TestRow(
      1740009523401L,
      1740009523401L,
      "myengine.com",
      "WRITE",
      Map("mode" -> "Append", "partitionBy" -> "[]"),
      null, // isBlindAppend is missing from JSON, should be null
      "cb009f42-5da1-4e7e-b4fa-09de3332f52a",
      Map("numFiles" -> "1"))

    checkAnswer(Seq(actResult), Seq(expResult))
  }

  test("fromColumnVector handles null isBlindAppend from parsed JSON without NPE") {
    val input =
      """
        |{
        |   "timestamp":1740009523401,
        |   "engineInfo":"myengine.com",
        |   "operation":"WRITE",
        |   "operationParameters":{},
        |   "txnId":"test-txn-id",
        |   "operationMetrics":{}
        |}
        |""".stripMargin

    val readSchema = new StructType().add("commitInfo", CommitInfo.FULL_SCHEMA)
    val output = jsonHandler.parseJson(
      stringVector(Seq(s"""{"commitInfo":${input.trim}}""")),
      readSchema,
      Optional.empty())
    assert(output.getSize == 1)
    val commitInfoVector = output.getColumnVector(0)
    val commitInfo = CommitInfo.fromColumnVector(commitInfoVector, 0)

    assert(commitInfo != null)
    assert(commitInfo.getIsBlindAppend === Optional.empty())
    assert(commitInfo.getInCommitTimestamp === Optional.empty())
    assert(commitInfo.getTimestamp === 1740009523401L)
    assert(commitInfo.getEngineInfo === Optional.of("myengine.com"))
    assert(commitInfo.getOperation === Optional.of("WRITE"))
    assert(commitInfo.getTxnId === Optional.of("test-txn-id"))
  }

  test("fromColumnVector handles null engineInfo, operation, and txnId without NPE") {
    // Simulates a commit written by an external engine that omits these optional fields
    val input =
      """
        |{
        |   "timestamp":1740009523401,
        |   "operationParameters":{},
        |   "operationMetrics":{}
        |}
        |""".stripMargin

    val readSchema = new StructType().add("commitInfo", CommitInfo.FULL_SCHEMA)
    val output = jsonHandler.parseJson(
      stringVector(Seq(s"""{"commitInfo":${input.trim}}""")),
      readSchema,
      Optional.empty())
    assert(output.getSize == 1)
    val commitInfoVector = output.getColumnVector(0)
    val commitInfo = CommitInfo.fromColumnVector(commitInfoVector, 0)

    assert(commitInfo != null)
    assert(commitInfo.getEngineInfo === Optional.empty())
    assert(commitInfo.getOperation === Optional.empty())
    assert(commitInfo.getTxnId === Optional.empty())
    assert(commitInfo.getIsBlindAppend === Optional.empty())
    assert(commitInfo.getInCommitTimestamp === Optional.empty())
    assert(commitInfo.getTimestamp === 1740009523401L)
    assert(commitInfo.getOperationParameters.isEmpty)
    assert(commitInfo.getOperationMetrics.isEmpty)
  }

  test("fromColumnVector with only timestamp field does not NPE") {
    // Minimal commit info - only the required timestamp field
    val input =
      """
        |{
        |   "timestamp":1000
        |}
        |""".stripMargin

    val readSchema = new StructType().add("commitInfo", CommitInfo.FULL_SCHEMA)
    val output = jsonHandler.parseJson(
      stringVector(Seq(s"""{"commitInfo":${input.trim}}""")),
      readSchema,
      Optional.empty())
    assert(output.getSize == 1)
    val commitInfoVector = output.getColumnVector(0)
    val commitInfo = CommitInfo.fromColumnVector(commitInfoVector, 0)

    assert(commitInfo != null)
    assert(commitInfo.getTimestamp === 1000L)
    assert(commitInfo.getEngineInfo === Optional.empty())
    assert(commitInfo.getOperation === Optional.empty())
    assert(commitInfo.getTxnId === Optional.empty())
    assert(commitInfo.getIsBlindAppend === Optional.empty())
    assert(commitInfo.getInCommitTimestamp === Optional.empty())
    assert(commitInfo.getOperationParameters.isEmpty)
    assert(commitInfo.getOperationMetrics.isEmpty)
  }

  test("CommitInfo round-trips through toRow with nullable fields") {
    val commitInfo = new CommitInfo(
      Optional.of(100L),
      200L,
      Optional.empty(), // engineInfo
      Optional.empty(), // operation
      Collections.emptyMap(),
      Optional.empty(), // isBlindAppend
      Optional.empty(), // txnId
      Collections.emptyMap())

    val row = commitInfo.toRow()
    assert(row.isNullAt(CommitInfo.FULL_SCHEMA.indexOf("engineInfo")))
    assert(row.isNullAt(CommitInfo.FULL_SCHEMA.indexOf("operation")))
    assert(row.isNullAt(CommitInfo.FULL_SCHEMA.indexOf("txnId")))
    assert(row.isNullAt(CommitInfo.FULL_SCHEMA.indexOf("isBlindAppend")))
    assert(row.getLong(CommitInfo.FULL_SCHEMA.indexOf("inCommitTimestamp")) === 100L)
    assert(row.getLong(CommitInfo.FULL_SCHEMA.indexOf("timestamp")) === 200L)
  }
}
