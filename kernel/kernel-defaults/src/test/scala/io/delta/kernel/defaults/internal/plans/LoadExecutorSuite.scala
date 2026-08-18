/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.plans

import java.io.ByteArrayInputStream
import java.lang.{Long => LongJ}
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.Optional
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ColumnarBatch, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector
import io.delta.kernel.engine.{FileReadRequest, FileReadResult, FileSystemClient}
import io.delta.kernel.expressions.{Column, Predicate}
import io.delta.kernel.internal.actions.DeletionVectorDescriptor
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils
import io.delta.kernel.internal.plans.{FileType, Load, LoadColumnFileMeta, Values}
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.test.{BaseMockFileSystemClient, BaseMockJsonHandler, BaseMockParquetHandler, MockEngineUtils}
import io.delta.kernel.types._
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class LoadExecutorSuite extends AnyFunSuite with MockEngineUtils with BeforeAndAfterAll {
  private val ioExecutor = Executors.newFixedThreadPool(8)

  override protected def afterAll(): Unit = {
    ioExecutor.shutdownNow()
    assert(ioExecutor.awaitTermination(10, TimeUnit.SECONDS))
    super.afterAll()
  }

  private val metadataSchema = new StructType()
    .add("path", StringType.STRING, false)
    .add("size", LongType.LONG, true)
    .add("num_records", LongType.LONG, true)
    .add("part", StringType.STRING, true)
    .add("dv", DeletionVectorDescriptor.READ_SCHEMA, true)

  private val nullablePathMetadataSchema = new StructType()
    .add("path", StringType.STRING, true)
    .add("size", LongType.LONG, true)
    .add("num_records", LongType.LONG, true)
    .add("part", StringType.STRING, true)
    .add("dv", DeletionVectorDescriptor.READ_SCHEMA, true)

  test("Load accepts non-null values from a nullable path column") {
    val outputSchema = new StructType().add("id", LongType.LONG, false)
    val input = new TrackingIterator(Seq(valuesBatch(
      nullablePathMetadataSchema,
      Seq(GenericRow.fromValues(
        nullablePathMetadataSchema,
        Seq("data.parquet", LongJ.valueOf(10), null, null, null).asJava)))))
    val handler = new TrackingParquetHandler((_, schema) =>
      Seq(rowBatch(schema, Seq(Seq(LongJ.valueOf(1))))))
    val load = new Load(
      outputSchema,
      FileType.PARQUET,
      Optional.of(new URI("file:///table/")),
      Seq.empty[String].asJava,
      fileMeta(),
      new Column("dv"))

    val result = LoadExecutor.execute(
      load,
      nullablePathMetadataSchema,
      input,
      mockEngine(parquetHandler = handler),
      ioExecutor)

    assert(collect(result)(_.getLong(0)) === Seq(1L))
    assert(handler.calls.map(_.files.head.getPath) === Seq("file:/table/data.parquet"))
  }

  test("Load rejects a selected null path value") {
    val outputSchema = new StructType().add("id", LongType.LONG, false)
    val input = new TrackingIterator(Seq(valuesBatch(
      nullablePathMetadataSchema,
      Seq(GenericRow.fromValues(
        nullablePathMetadataSchema,
        Seq(null, LongJ.valueOf(10), null, null, null).asJava)))))
    val handler = new TrackingParquetHandler((_, schema) => Seq(rowBatch(schema, Seq.empty)))
    val load = new Load(
      outputSchema,
      FileType.PARQUET,
      Optional.of(new URI("file:///table/")),
      Seq.empty[String].asJava,
      fileMeta(),
      new Column("dv"))

    val error = intercept[IllegalArgumentException] {
      LoadExecutor.execute(
        load,
        nullablePathMetadataSchema,
        input,
        mockEngine(parquetHandler = handler),
        ioExecutor)
    }

    assert(error.getMessage === "Load path must not be null")
    assert(handler.calls.isEmpty)
    assert(input.closeCalls === 1)
  }

  test("Parquet resolves metadata and dispatches status and data IO concurrently") {
    val outputSchema = new StructType()
      .add("id", LongType.LONG, false)
      .add("part", StringType.STRING, true)
    val relative = "file:/table/first.parquet"
    val absolute = "file:///other/second.parquet"
    val inputBatch = valuesBatch(
      metadataSchema,
      Seq(
        metadataRow("first.parquet", null, "a", null),
        metadataRow(absolute, null, "b", null),
        metadataRow("ignored.parquet", LongJ.valueOf(-1), "ignored", null)))
    val selected = new FilteredColumnarBatch(
      inputBatch.getData,
      Optional.of(new DefaultBooleanVector(
        3,
        Optional.empty(),
        Array(true, true, false))))
    val input = new TrackingIterator(Seq(selected))
    val handler = new TrackingParquetHandler((path, schema) =>
      path match {
        case `relative` => Seq(rowBatch(schema, Seq(Seq(LongJ.valueOf(1)))))
        case `absolute` => Seq(rowBatch(schema, Seq(Seq(LongJ.valueOf(2)))))
      })
    val statusPaths = new ConcurrentLinkedQueue[String]
    val statusLookups = new CountDownLatch(2)
    val fileSystem = new BaseMockFileSystemClient {
      override def getFileStatus(path: String): FileStatus = {
        statusPaths.add(path)
        statusLookups.countDown()
        assert(statusLookups.await(10, TimeUnit.SECONDS))
        FileStatus.of(path, 20, 30)
      }
    }
    val load = new Load(
      outputSchema,
      FileType.PARQUET,
      Optional.of(new URI("file:///table/")),
      Seq("part").asJava,
      fileMeta(),
      new Column("dv"))

    val result = LoadExecutor.execute(
      load,
      metadataSchema,
      input,
      mockEngine(fileSystemClient = fileSystem, parquetHandler = handler),
      ioExecutor)

    assert(input.closeCalls === 1)
    val rows = collect(result)(row => row.getLong(0) -> row.getString(1))
    assert(handler.calls.map(_.files.head.getPath).toSet === Set(relative, absolute))
    assert(handler.calls.map(_.files.head.getSize).toSet === Set(20L))
    assert(statusPaths.asScala.toSet === Set(relative, absolute))
    assert(rows === Seq(1L -> "a", 2L -> "b"))
  }

  test("JSON resolves nested metadata and preserves a requested row index") {
    val rowIndex = StructField.createMetadataColumn("row_index", MetadataColumnSpec.ROW_INDEX)
    val outputSchema = new StructType()
      .add("id", LongType.LONG, false)
      .add(rowIndex)
      .add("version", LongType.LONG, false)
    val nestedMetadata = new StructType()
      .add("path", StringType.STRING, false)
      .add("size", LongType.LONG, true)
      .add("num_records", LongType.LONG, true)
      .add("dv", DeletionVectorDescriptor.READ_SCHEMA, true)
    val inputSchema = new StructType()
      .add("meta", nestedMetadata, false)
      .add("version", LongType.LONG, false)
    val meta = GenericRow.fromValues(
      nestedMetadata,
      Seq("rows.json", LongJ.valueOf(10), null, null).asJava)
    val input = new TrackingIterator(Seq(valuesBatch(
      inputSchema,
      Seq(GenericRow.fromValues(inputSchema, Seq(meta, LongJ.valueOf(7)).asJava)))))
    val handler = new TrackingJsonHandler((_, schema) =>
      Seq(
        rowBatch(schema, Seq(Seq(LongJ.valueOf(1)), Seq(LongJ.valueOf(2)))),
        rowBatch(schema, Seq(Seq(LongJ.valueOf(3))))))
    val load = new Load(
      outputSchema,
      FileType.JSON,
      Optional.of(new URI("file:///table/")),
      Seq("version").asJava,
      new LoadColumnFileMeta(
        new Column(Array("meta", "path")),
        new Column(Array("meta", "size")),
        new Column(Array("meta", "num_records"))),
      new Column(Array("meta", "dv")))

    val result = LoadExecutor.execute(
      load,
      inputSchema,
      input,
      mockEngine(jsonHandler = handler),
      ioExecutor)

    val rows = collect(result)(row => (row.getLong(0), row.getLong(1), row.getLong(2)))
    assert(handler.calls.size === 1)
    assert(rows ===
      Seq((1L, 0L, 7L), (2L, 1L, 7L), (3L, 2L, 7L)))
  }

  test("deletion vectors use either a private or requested row index") {
    val tableRoot = getClass.getResource("/basic-dv-with-checkpoint").toURI
    val descriptor = new DeletionVectorDescriptor(
      DeletionVectorDescriptor.UUID_DV_MARKER,
      "2FtLtJDE!.VZ.+udLGa0",
      Optional.empty(),
      34,
      1)
    val fileSystem = resourceFileSystem()
    val deleted = DeletionVectorUtils
      .loadNewDvAndBitmap(mockEngine(fileSystemClient = fileSystem), tableRoot.toString, descriptor)
      ._2
      .toArray
      .toSet
    val expected = (0L until 250L).filterNot(deleted)

    for (requestedRowIndex <- Seq(false, true)) {
      val outputSchema = if (requestedRowIndex) {
        new StructType()
          .add("id", LongType.LONG, false)
          .add(StructField.createMetadataColumn("index", MetadataColumnSpec.ROW_INDEX))
      } else {
        new StructType().add("id", LongType.LONG, false)
      }
      val handler = new TrackingParquetHandler((_, schema) =>
        Seq(rowBatch(
          schema,
          (0L until 250L).map { value =>
            if (schema.length() == 1) Seq(LongJ.valueOf(value))
            else Seq(LongJ.valueOf(value), LongJ.valueOf(value))
          })))
      val input = new TrackingIterator(Seq(valuesBatch(
        metadataSchema,
        Seq(metadataRow("data.parquet", LongJ.valueOf(10), "a", descriptor.toRow)))))
      val load = new Load(
        outputSchema,
        FileType.PARQUET,
        Optional.of(tableRoot),
        Seq.empty[String].asJava,
        fileMeta(),
        new Column("dv"))
      val result = LoadExecutor.execute(
        load,
        metadataSchema,
        input,
        mockEngine(fileSystemClient = fileSystem, parquetHandler = handler),
        ioExecutor)

      val actual = collect(result)(row =>
        row.getLong(0) -> (if (requestedRowIndex) Some(row.getLong(1)) else None))
      assert(actual.map(_._1) === expected)
      if (requestedRowIndex) {
        assert(actual.map(_._2.get) === expected)
      }
    }
  }

  test("deletion vector reads share the Load IO frontier") {
    val tableRoot = getClass.getResource("/basic-dv-with-checkpoint").toURI
    val descriptor = new DeletionVectorDescriptor(
      DeletionVectorDescriptor.UUID_DV_MARKER,
      "2FtLtJDE!.VZ.+udLGa0",
      Optional.empty(),
      34,
      1)
    val readsStarted = new CountDownLatch(4)
    val fileSystem = resourceFileSystem(() => {
      readsStarted.countDown()
      assert(readsStarted.await(10, TimeUnit.SECONDS))
    })
    val outputSchema = new StructType().add("id", LongType.LONG, false)
    val handler = new TrackingParquetHandler(
      (_, schema) =>
        Seq(rowBatch(
          schema,
          (0L until 2L).map(value => Seq(LongJ.valueOf(value), LongJ.valueOf(value))))),
      () => {
        readsStarted.countDown()
        assert(readsStarted.await(10, TimeUnit.SECONDS))
      })
    val input = new TrackingIterator(Seq(valuesBatch(
      metadataSchema,
      Seq(
        metadataRow("a.parquet", LongJ.valueOf(10), "a", descriptor.toRow),
        metadataRow("b.parquet", LongJ.valueOf(10), "b", descriptor.toRow)))))
    val load = new Load(
      outputSchema,
      FileType.PARQUET,
      Optional.of(tableRoot),
      Seq.empty[String].asJava,
      fileMeta(),
      new Column("dv"))

    val result = LoadExecutor.execute(
      load,
      metadataSchema,
      input,
      mockEngine(fileSystemClient = fileSystem, parquetHandler = handler),
      ioExecutor)

    assert(readsStarted.await(10, TimeUnit.SECONDS))
    result.close()
  }

  test("validation happens before input ownership and selected metadata fails before data IO") {
    val outputSchema = new StructType().add("id", LongType.LONG, false)
    val input = new TrackingIterator(Seq(valuesBatch(
      metadataSchema,
      Seq(metadataRow("data.parquet", LongJ.valueOf(10), "a", null)))))
    val invalid = new Load(
      outputSchema,
      FileType.PARQUET,
      Optional.of(new URI("file:///table/")),
      Seq.empty[String].asJava,
      new LoadColumnFileMeta(
        new Column("missing"),
        new Column("size"),
        new Column("num_records")),
      new Column("dv"))

    assertThrows[IllegalArgumentException] {
      LoadExecutor.execute(invalid, metadataSchema, input, mockEngine(), ioExecutor)
    }
    assert(input.hasNextCalls === 0)
    assert(input.closeCalls === 0)

    val badInput = new TrackingIterator(Seq(valuesBatch(
      metadataSchema,
      Seq(metadataRow("data.parquet", LongJ.valueOf(-1), "a", null)))))
    val handler = new TrackingParquetHandler((_, schema) => Seq(rowBatch(schema, Seq.empty)))
    val valid = new Load(
      outputSchema,
      FileType.PARQUET,
      Optional.of(new URI("file:///table/")),
      Seq.empty[String].asJava,
      fileMeta(),
      new Column("dv"))

    assertThrows[IllegalArgumentException] {
      LoadExecutor.execute(
        valid,
        metadataSchema,
        badInput,
        mockEngine(parquetHandler = handler),
        ioExecutor)
    }
    assert(badInput.closeCalls === 1)
    assert(handler.calls.isEmpty)
  }

  test("invalid selected metadata does not leave an untracked status lookup") {
    val malformedDv = GenericRow.fromValues(
      DeletionVectorDescriptor.READ_SCHEMA,
      Seq(null, "inline", null, Integer.valueOf(1), LongJ.valueOf(1)).asJava)
    val input = new TrackingIterator(Seq(valuesBatch(
      metadataSchema,
      Seq(metadataRow("data.parquet", null, "a", malformedDv)))))
    var statusLookups = 0
    val fileSystem = new BaseMockFileSystemClient {
      override def getFileStatus(path: String): FileStatus = {
        statusLookups += 1
        FileStatus.of(path, 10, 0)
      }
    }
    val load = new Load(
      new StructType().add("id", LongType.LONG, false),
      FileType.PARQUET,
      Optional.of(new URI("file:///table/")),
      Seq.empty[String].asJava,
      fileMeta(),
      new Column("dv"))

    assertThrows[IllegalArgumentException] {
      LoadExecutor.execute(
        load,
        metadataSchema,
        input,
        mockEngine(fileSystemClient = fileSystem),
        ioExecutor)
    }
    assert(statusLookups === 0)
    assert(input.closeCalls === 1)
  }

  test("deletion-vector mapping failure closes every file reader") {
    val tableRoot = getClass.getResource("/basic-dv-with-checkpoint").toURI
    val descriptor = new DeletionVectorDescriptor(
      DeletionVectorDescriptor.UUID_DV_MARKER,
      "2FtLtJDE!.VZ.+udLGa0",
      Optional.empty(),
      34,
      1)
    val handler = new TrackingParquetHandler((path, schema) => {
      val batch = rowBatch(
        schema,
        Seq(Seq(LongJ.valueOf(1), LongJ.valueOf(0))))
      if (path.endsWith("a.parquet")) Seq(nonSpliceable(batch)) else Seq(batch)
    })
    val input = new TrackingIterator(Seq(valuesBatch(
      metadataSchema,
      Seq(
        metadataRow("a.parquet", LongJ.valueOf(10), "a", descriptor.toRow),
        metadataRow("b.parquet", LongJ.valueOf(10), "b", descriptor.toRow)))))
    val load = new Load(
      new StructType().add("id", LongType.LONG, false),
      FileType.PARQUET,
      Optional.of(tableRoot),
      Seq.empty[String].asJava,
      fileMeta(),
      new Column("dv"))
    val result = LoadExecutor.execute(
      load,
      metadataSchema,
      input,
      mockEngine(fileSystemClient = resourceFileSystem(), parquetHandler = handler),
      ioExecutor)

    assertThrows[UnsupportedOperationException](result.next())
    assert(handler.readers.forall(_.closeCalls === 1))
  }

  test("Load starts every file read before consumption and closes unconsumed readers") {
    val outputSchema = new StructType().add("id", LongType.LONG, false)
    val input = new TrackingIterator(Seq(valuesBatch(
      metadataSchema,
      Seq(
        metadataRow("a.parquet", LongJ.valueOf(10), "a", null),
        metadataRow("b.parquet", LongJ.valueOf(10), "b", null)))))
    val readsStarted = new CountDownLatch(2)
    val releaseReads = new CountDownLatch(1)
    val handler = new TrackingParquetHandler(
      (_, schema) => Seq(rowBatch(schema, Seq(Seq(LongJ.valueOf(1))))),
      () => {
        readsStarted.countDown()
        releaseReads.await()
      })
    val load = new Load(
      outputSchema,
      FileType.PARQUET,
      Optional.of(new URI("file:///table/")),
      Seq.empty[String].asJava,
      fileMeta(),
      new Column("dv"))

    val result = LoadExecutor.execute(
      load,
      metadataSchema,
      input,
      mockEngine(parquetHandler = handler),
      ioExecutor)
    assert(readsStarted.await(10, TimeUnit.SECONDS))
    assert(handler.readers.size === 2)
    assert(handler.readers.forall(_.hasNextCalls === 1))

    releaseReads.countDown()
    result.close()
    assert(handler.readers.forall(_.closeCalls === 1))
  }

  private def metadataRow(
      path: String,
      size: java.lang.Long,
      part: String,
      dv: Row): Row =
    GenericRow.fromValues(
      metadataSchema,
      Seq(path, size, null, part, dv).asJava)

  private def fileMeta(): LoadColumnFileMeta = new LoadColumnFileMeta(
    new Column("path"),
    new Column("size"),
    new Column("num_records"))

  private def valuesBatch(schema: StructType, rows: Seq[Row]): FilteredColumnarBatch = {
    val iterator = ValuesExecutor.execute(new Values(schema, rows.asJava))
    try iterator.next()
    finally iterator.close()
  }

  private def rowBatch(schema: StructType, values: Seq[Seq[Any]]): ColumnarBatch =
    new DefaultRowBasedColumnarBatch(
      schema,
      values.map[Row](row => GenericRow.fromValues(schema, row.asJava)).asJava)

  private def nonSpliceable(delegate: ColumnarBatch): ColumnarBatch = new ColumnarBatch {
    override def getSchema: StructType = delegate.getSchema
    override def getColumnVector(ordinal: Int) = delegate.getColumnVector(ordinal)
    override def getSize: Int = delegate.getSize
  }

  private def collect[T](
      iterator: CloseableIterator[FilteredColumnarBatch])(read: Row => T): Seq[T] = {
    iterator.toInMemoryList().asScala.toSeq.flatMap { batch =>
      val rows = batch.getRows
      try rows.asScala.map(read).toSeq
      finally rows.close()
    }
  }

  private def resourceFileSystem(
      onRead: () => Unit = () => ()): FileSystemClient = new BaseMockFileSystemClient {
    override def readFiles(
        requests: CloseableIterator[FileReadRequest])
        : CloseableIterator[ByteArrayInputStream] = {
      val request = requests.next()
      requests.close()
      onRead()
      val bytes = Files.readAllBytes(Paths.get(URI.create(request.getPath)))
      val start = request.getStartOffset
      val data = java.util.Arrays.copyOfRange(bytes, start, start + request.getReadLength)
      Utils.singletonCloseableIterator(new ByteArrayInputStream(data))
    }
  }

  private case class ReadCall(files: Seq[FileStatus], schema: StructType)

  private class TrackingParquetHandler(
      output: (String, StructType) => Seq[ColumnarBatch],
      onHasNext: () => Unit = () => ())
      extends BaseMockParquetHandler {
    val calls = ArrayBuffer.empty[ReadCall]
    val readers = ArrayBuffer.empty[TrackingIterator[FileReadResult]]

    override def readParquetFiles(
        files: CloseableIterator[FileStatus],
        schema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = {
      val requested = files.toInMemoryList().asScala.toSeq
      this.synchronized {
        calls += ReadCall(requested, schema)
      }
      val reader = new TrackingIterator(
        requested.flatMap(file =>
          output(file.getPath, schema).map(batch => new FileReadResult(batch, file.getPath))),
        onHasNext)
      this.synchronized {
        readers += reader
      }
      reader
    }
  }

  private class TrackingJsonHandler(
      output: (String, StructType) => Seq[ColumnarBatch])
      extends BaseMockJsonHandler {
    val calls = ArrayBuffer.empty[ReadCall]

    override def readJsonFiles(
        files: CloseableIterator[FileStatus],
        schema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
      val requested = files.toInMemoryList().asScala.toSeq
      this.synchronized {
        calls += ReadCall(requested, schema)
      }
      new TrackingIterator(requested.flatMap(file => output(file.getPath, schema)))
    }
  }

  private class TrackingIterator[T](
      values: Seq[T],
      onHasNext: () => Unit = () => ())
      extends CloseableIterator[T] {
    private var index = 0
    @volatile var hasNextCalls = 0
    var closeCalls = 0

    override def hasNext: Boolean = {
      hasNextCalls += 1
      onHasNext()
      index < values.size
    }

    override def next(): T = {
      if (!hasNext) throw new NoSuchElementException
      val value = values(index)
      index += 1
      value
    }

    override def close(): Unit = closeCalls += 1
  }
}
