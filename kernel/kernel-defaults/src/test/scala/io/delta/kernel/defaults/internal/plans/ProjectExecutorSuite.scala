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

import java.util
import java.util.{NoSuchElementException, Optional}

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ColumnarBatch, ColumnVector, FilteredColumnarBatch}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.{DefaultGenericVector, DefaultStructVector}
import io.delta.kernel.expressions._
import io.delta.kernel.internal.plans.Project
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class ProjectExecutorSuite extends AnyFunSuite {
  private val intSchema = new StructType()
    .add("a", IntegerType.INTEGER, true)
    .add("b", IntegerType.INTEGER, false)

  private def vector(dataType: DataType, values: AnyRef*): ColumnVector =
    DefaultGenericVector.fromArray(dataType, values.toArray)

  private def dataBatch(
      schema: StructType,
      columns: ColumnVector*): ColumnarBatch = {
    val size = columns.headOption.map(_.getSize).getOrElse(0)
    require(columns.forall(_.getSize == size))
    new DefaultColumnarBatch(size, schema, columns.toArray)
  }

  private def intBatch(a: Seq[Integer], b: Seq[Integer]): ColumnarBatch =
    dataBatch(
      intSchema,
      vector(IntegerType.INTEGER, a: _*),
      vector(IntegerType.INTEGER, b: _*))

  private def filtered(data: ColumnarBatch): FilteredColumnarBatch =
    new FilteredColumnarBatch(data, Optional.empty())

  private def denseProject(
      expressions: Seq[Expression],
      schema: StructType): Project =
    new Project(new StructExpression(expressions.asJava), schema)

  private def identityProject(schema: StructType): Project =
    denseProject(schema.fields().asScala.map(field => new Column(field.getName)).toSeq, schema)

  private def ints(vector: ColumnVector): Seq[Integer] =
    (0 until vector.getSize).map { rowId =>
      if (vector.isNullAt(rowId)) null else Int.box(vector.getInt(rowId))
    }

  private def execute(
      project: Project,
      inputSchema: StructType,
      input: TrackingIterator): CloseableIterator[FilteredColumnarBatch] =
    ProjectExecutor.execute(project, inputSchema, input)

  test("dense projection evaluates expressions and uses declared field names") {
    val nestedSchema = new StructType().add("value", IntegerType.INTEGER, false)
    val outputSchema = new StructType()
      .add("filled", IntegerType.INTEGER, false)
      .add("incremented", IntegerType.INTEGER, false)
      .add("nested", nestedSchema, false)
    val coalesce = new ScalarExpression(
      "COALESCE",
      util.Arrays.asList(new Column("a"), Literal.ofInt(10)))
    val add = new ScalarExpression(
      "ADD",
      util.Arrays.asList(new Column("b"), Literal.ofInt(1)))
    val nested = new StructExpression(Seq[Expression](new Column("b")).asJava)
    val input = new TrackingIterator(Seq(filtered(intBatch(
      Seq(null, Int.box(2)),
      Seq(Int.box(3), Int.box(4))))))

    val result = execute(
      denseProject(Seq(coalesce, add, nested), outputSchema),
      intSchema,
      input).next()

    assert(result.getData.getSchema == outputSchema)
    assert(ints(result.getData.getColumnVector(0)) == Seq(10, 2))
    assert(ints(result.getData.getColumnVector(1)) == Seq(4, 5))
    assert(ints(result.getData.getColumnVector(2).getChild(0)) == Seq(3, 4))
  }

  test("struct patch preserves Rust field ordering for passthrough, replace, and drop") {
    val inputSchema = new StructType()
      .add("a", IntegerType.INTEGER, false)
      .add("b", IntegerType.INTEGER, false)
      .add("c", IntegerType.INTEGER, false)
    val transforms = new util.LinkedHashMap[String, StructPatch.FieldTransform]()
    transforms.put(
      "b",
      new StructPatch.FieldTransform(Seq[Expression](Literal.ofInt(9)).asJava, true, false))
    transforms.put(
      "c",
      new StructPatch.FieldTransform(
        util.Collections.emptyList[Expression](),
        true,
        false))
    val patch = new StructPatch(
      Optional.empty(),
      transforms,
      Seq[Expression](Literal.ofInt(0)).asJava,
      Seq[Expression](Literal.ofInt(99)).asJava)
    val outputSchema = new StructType()
      .add("pre", IntegerType.INTEGER, false)
      .add("a", IntegerType.INTEGER, false)
      .add("replaced_b", IntegerType.INTEGER, false)
      .add("tail", IntegerType.INTEGER, false)
    val input = new TrackingIterator(Seq(filtered(dataBatch(
      inputSchema,
      vector(IntegerType.INTEGER, Int.box(1), Int.box(2)),
      vector(IntegerType.INTEGER, Int.box(10), Int.box(20)),
      vector(IntegerType.INTEGER, Int.box(100), Int.box(200))))))

    val result = execute(new Project(patch, outputSchema), inputSchema, input).next().getData

    assert((0 until outputSchema.length()).map(i => ints(result.getColumnVector(i))) == Seq(
      Seq(0, 0),
      Seq(1, 2),
      Seq(9, 9),
      Seq(99, 99)))
  }

  test("projection preserves the selection vector and batch metadata without compacting rows") {
    val data = intBatch(
      Seq(Int.box(1), Int.box(2), Int.box(3)),
      Seq(Int.box(10), Int.box(20), Int.box(30)))
    val selection = vector(
      BooleanType.BOOLEAN,
      java.lang.Boolean.FALSE,
      java.lang.Boolean.TRUE,
      java.lang.Boolean.FALSE)
    val source = new FilteredColumnarBatch(data, Optional.of(selection), "file:///a.parquet", 1)
    val input = new TrackingIterator(Seq(source))

    val result = execute(identityProject(intSchema), intSchema, input).next()

    assert(result.getData.getSize == 3)
    assert(result.getSelectionVector.get() eq selection)
    assert(result.getFilePath.get() == "file:///a.parquet")
    assert(result.getPreComputedNumSelectedRows.get() == 1)
    val rows = result.getRows
    assert(rows.next().getInt(0) == 2)
    assert(!rows.hasNext)
    rows.close()
  }

  test("execution is lazy, emits one batch per input batch, and keeps empty batches") {
    val batches = Seq(
      filtered(intBatch(Seq(Int.box(1)), Seq(Int.box(10)))),
      filtered(intBatch(Seq.empty, Seq.empty)),
      filtered(intBatch(Seq(Int.box(2)), Seq(Int.box(20)))))
    val input = new TrackingIterator(batches)
    val output = execute(identityProject(intSchema), intSchema, input)

    assert(input.hasNextCalls == 0)
    assert(input.nextCalls == 0)
    assert(output.next().getData.getSize == 1)
    assert(input.nextCalls == 1)
    assert(output.next().getData.getSize == 0)
    assert(input.nextCalls == 2)
    assert(output.next().getData.getSize == 1)
    assert(input.nextCalls == 3)
    assert(!output.hasNext)
    output.close()
    assert(input.closeCount == 1)
  }

  test("projection supports a zero-column result") {
    val outputSchema = new StructType()
    val input = new TrackingIterator(Seq(filtered(intBatch(
      Seq(Int.box(1)),
      Seq(Int.box(10))))))

    val result = execute(denseProject(Seq.empty, outputSchema), intSchema, input).next().getData

    assert(result.getSchema == outputSchema)
    assert(result.getSize == 1)
  }

  test("closing an unconsumed projection closes its input") {
    val input = new TrackingIterator(Seq(filtered(intBatch(
      Seq(Int.box(1)),
      Seq(Int.box(10))))))
    val output = execute(identityProject(intSchema), intSchema, input)

    output.close()

    assert(input.nextCalls == 0)
    assert(input.closeCount == 1)
  }

  Seq[(String, FilteredColumnarBatch, Class[_ <: RuntimeException])](
    (
      "wrong input schema",
      filtered(dataBatch(
        new StructType().add("other", IntegerType.INTEGER),
        vector(IntegerType.INTEGER, Int.box(1)))),
      classOf[IllegalArgumentException]),
    (
      "null batch",
      null,
      classOf[NullPointerException])).foreach { case (name, batch, errorClass) =>
    test(s"$name fails lazily and leaves the stream closeable") {
      val input = new TrackingIterator(Seq(batch))
      val output = execute(identityProject(intSchema), intSchema, input)
      assert(input.nextCalls == 0)

      val error = intercept[RuntimeException] {
        output.next()
      }
      assert(errorClass.isInstance(error))
      output.close()
      assert(input.closeCount == 1)
    }
  }

  test("invalid evaluator setup fails before consuming or closing input") {
    val badOutput = new StructType().add("bad", StringType.STRING)
    val project = denseProject(Seq(new Column("a")), badOutput)
    val input = new TrackingIterator(Seq(filtered(intBatch(
      Seq(Int.box(1)),
      Seq(Int.box(10))))))

    intercept[UnsupportedOperationException] {
      execute(project, intSchema, input)
    }

    assert(input.hasNextCalls == 0)
    assert(input.nextCalls == 0)
    assert(input.closeCount == 0)
  }

  test("nullable struct roots mask every projected field") {
    val denseInputSchema = new StructType()
      .add("value", IntegerType.INTEGER, false)
      .add("keep", BooleanType.BOOLEAN, true)
    val denseInput = dataBatch(
      denseInputSchema,
      vector(IntegerType.INTEGER, Int.box(1), Int.box(2), Int.box(3)),
      vector(
        BooleanType.BOOLEAN,
        java.lang.Boolean.TRUE,
        java.lang.Boolean.FALSE,
        null))
    val denseOutputSchema = new StructType()
      .add("value", IntegerType.INTEGER, false)
      .add("constant", IntegerType.INTEGER, false)
    val denseExpression = new StructExpression(
      Seq[Expression](new Column("value"), Literal.ofInt(9)).asJava,
      new Column("keep"))

    val denseResult = execute(
      new Project(denseExpression, denseOutputSchema),
      denseInputSchema,
      new TrackingIterator(Seq(filtered(denseInput)))).next().getData
    assert(ints(denseResult.getColumnVector(0)) == Seq(Int.box(1), null, null))
    assert(ints(denseResult.getColumnVector(1)) == Seq(Int.box(9), null, null))

    val innerType = new StructType().add("x", IntegerType.INTEGER, false)
    val inner = new DefaultStructVector(
      2,
      innerType,
      Optional.empty(),
      Array(vector(IntegerType.INTEGER, Int.box(7), Int.box(8))))
    val outerType = new StructType().add("inner", innerType, false)
    val outer = new DefaultStructVector(
      2,
      outerType,
      Optional.of(Array(false, true)),
      Array(inner))
    val nestedInputSchema = new StructType().add("outer", outerType, true)
    val nestedInput = dataBatch(nestedInputSchema, outer)
    val nestedPatch = new StructPatch(
      Optional.of(new Column(Array("outer", "inner"))),
      util.Collections.emptyMap[String, StructPatch.FieldTransform](),
      util.Collections.emptyList[Expression](),
      util.Collections.emptyList[Expression]())

    val nestedResult = execute(
      new Project(nestedPatch, innerType),
      nestedInputSchema,
      new TrackingIterator(Seq(filtered(nestedInput)))).next().getData.getColumnVector(0)
    assert(ints(nestedResult) == Seq(Int.box(7), null))
  }

  private class TrackingIterator(values: Seq[FilteredColumnarBatch])
      extends CloseableIterator[FilteredColumnarBatch] {
    private var index = 0
    var hasNextCalls = 0
    var nextCalls = 0
    var closeCount = 0

    override def hasNext: Boolean = {
      hasNextCalls += 1
      index < values.size
    }

    override def next(): FilteredColumnarBatch = {
      if (index >= values.size) {
        throw new NoSuchElementException
      }
      nextCalls += 1
      val value = values(index)
      index += 1
      value
    }

    override def close(): Unit = closeCount += 1
  }
}
