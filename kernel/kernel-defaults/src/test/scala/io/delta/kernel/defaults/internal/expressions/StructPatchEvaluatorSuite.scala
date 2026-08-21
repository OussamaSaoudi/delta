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
package io.delta.kernel.defaults.internal.expressions

import java.util
import java.util.Optional

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ColumnarBatch, ColumnVector}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.{DefaultGenericVector, DefaultStructVector}
import io.delta.kernel.expressions._
import io.delta.kernel.internal.data.StructRow
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class StructPatchEvaluatorSuite extends AnyFunSuite {
  private def vector(dataType: DataType, values: AnyRef*): ColumnVector =
    DefaultGenericVector.fromArray(dataType, values.toArray)

  private def batch(fields: (StructField, ColumnVector)*): ColumnarBatch = {
    val rowCount = fields.headOption.map(_._2.getSize).getOrElse(0)
    new DefaultColumnarBatch(
      rowCount,
      new StructType(fields.map(_._1).asJava),
      fields.map(_._2).toArray)
  }

  private def evaluate(
      input: ColumnarBatch,
      expression: Expression,
      outputType: DataType): ColumnVector =
    new DefaultExpressionEvaluator(input.getSchema, expression, outputType).eval(input)

  private def fieldTransform(
      replace: Boolean,
      expressions: Seq[Expression] = Seq.empty,
      optional: Boolean = false): StructPatch.FieldTransform =
    new StructPatch.FieldTransform(expressions.asJava, replace, optional)

  private def patch(
      fields: util.Map[String, StructPatch.FieldTransform] = new util.LinkedHashMap(),
      prepended: Seq[Expression] = Seq.empty,
      appended: Seq[Expression] = Seq.empty,
      inputPath: Option[Column] = None): StructPatch = {
    val javaInputPath: Optional[Column] = inputPath match {
      case Some(path) => Optional.of(path)
      case None => Optional.empty()
    }
    new StructPatch(
      javaInputPath,
      fields,
      prepended.asJava,
      appended.asJava)
  }

  private def ints(vector: ColumnVector): Seq[Integer] =
    (0 until vector.getSize).map { rowId =>
      if (vector.isNullAt(rowId)) null else Integer.valueOf(vector.getInt(rowId))
    }

  private val baseSchema = new StructType()
    .add("a", IntegerType.INTEGER, false)
    .add("b", IntegerType.INTEGER, false)
    .add("c", IntegerType.INTEGER, false)

  private def baseBatch(): ColumnarBatch = batch(
    baseSchema.at(0) -> vector(IntegerType.INTEGER, Int.box(1), Int.box(2)),
    baseSchema.at(1) -> vector(IntegerType.INTEGER, Int.box(10), Int.box(20)),
    baseSchema.at(2) -> vector(IntegerType.INTEGER, Int.box(100), Int.box(200)))

  test("patch preserves Rust prepend, input, replacement, insertion, and append ordering") {
    val input = baseBatch()
    val transforms = new util.LinkedHashMap[String, StructPatch.FieldTransform]()
    transforms.put("c", fieldTransform(false, Seq(Literal.ofInt(7), Literal.ofInt(8))))
    transforms.put("a", fieldTransform(true, Seq(Literal.ofInt(9))))
    transforms.put("b", fieldTransform(true))
    val expression = patch(
      transforms,
      prepended = Seq(Literal.ofInt(0)),
      appended = Seq(Literal.ofInt(99)))
    val outputType = new StructType()
      .add("pre", IntegerType.INTEGER, false)
      .add("replaced_a", IntegerType.INTEGER, false)
      .add("c", IntegerType.INTEGER, false)
      .add("after_c_1", IntegerType.INTEGER, false)
      .add("after_c_2", IntegerType.INTEGER, false)
      .add("tail", IntegerType.INTEGER, false)

    val result = evaluate(input, expression, outputType)
    assert((0 until outputType.length()).map(i => ints(result.getChild(i))) == Seq(
      Seq(0, 0),
      Seq(9, 9),
      Seq(100, 200),
      Seq(7, 7),
      Seq(8, 8),
      Seq(99, 99)))
    result.close()
    assert(input.getColumnVector(2).getInt(0) == 100)
  }

  test("nested patch relocates a struct and lazily preserves its null rows") {
    val nestedType = new StructType()
      .add("x", IntegerType.INTEGER, false)
      .add("y", IntegerType.INTEGER, false)
    val nested = new DefaultStructVector(
      2,
      nestedType,
      Optional.of(Array(false, true)),
      Array(
        vector(IntegerType.INTEGER, Int.box(1), null),
        vector(IntegerType.INTEGER, Int.box(2), null)))
    val input = batch(new StructField("nested", nestedType, true) -> nested)
    val transforms = new util.LinkedHashMap[String, StructPatch.FieldTransform]()
    transforms.put("x", fieldTransform(true, Seq(Literal.ofInt(777))))
    transforms.put("y", fieldTransform(false, Seq(Literal.ofInt(555))))
    val outputType = new StructType()
      .add("x", IntegerType.INTEGER, false)
      .add("y", IntegerType.INTEGER, false)
      .add("after_y", IntegerType.INTEGER, false)

    val result = evaluate(
      input,
      patch(transforms, inputPath = Some(new Column("nested"))),
      outputType)
    assert(!result.isNullAt(0))
    assert(result.isNullAt(1))
    assert(ints(result.getChild(0)) == Seq(777, 777))
    assert(ints(result.getChild(1)) == Seq(Int.box(2), null))
    assert(ints(result.getChild(2)) == Seq(555, 555))
    assert(StructRow.fromStructVector(result, 1) == null)
    result.close()
  }

  test("nested patch expressions recurse through the same native lowering") {
    val nestedType = new StructType()
      .add("x", IntegerType.INTEGER, false)
      .add("y", IntegerType.INTEGER, false)
    val nested = new DefaultStructVector(
      2,
      nestedType,
      Optional.of(Array(false, true)),
      Array(
        vector(IntegerType.INTEGER, Int.box(1), null),
        vector(IntegerType.INTEGER, Int.box(2), null)))
    val input = batch(
      new StructField("nested", nestedType, true) -> nested,
      new StructField("id", IntegerType.INTEGER, false) ->
        vector(IntegerType.INTEGER, Int.box(10), Int.box(20)))
    val innerFields = new util.LinkedHashMap[String, StructPatch.FieldTransform]()
    innerFields.put("x", fieldTransform(true, Seq(Literal.ofInt(9))))
    val inner = patch(innerFields, inputPath = Some(new Column("nested")))
    val outerFields = new util.LinkedHashMap[String, StructPatch.FieldTransform]()
    outerFields.put("nested", fieldTransform(true, Seq(inner)))
    val outputType = new StructType()
      .add("patched", nestedType, true)
      .add("id", IntegerType.INTEGER, false)

    val result = evaluate(input, patch(outerFields), outputType)
    assert(StructRow.fromStructVector(result.getChild(0), 0).getInt(0) == 9)
    assert(StructRow.fromStructVector(result.getChild(0), 0).getInt(1) == 2)
    assert(result.getChild(0).isNullAt(1))
    assert(ints(result.getChild(1)) == Seq(10, 20))
    result.close()
  }

  Seq(
    "replace" -> fieldTransform(true, Seq(Literal.ofInt(7)), optional = true),
    "keep and insert" -> fieldTransform(false, Seq(Literal.ofInt(7)), optional = true),
    "drop" -> fieldTransform(true, optional = true)).foreach { case (name, transform) =>
    test(s"optional missing field ignores the entire transform: $name") {
      val transforms = new util.LinkedHashMap[String, StructPatch.FieldTransform]()
      transforms.put("missing", transform)
      val result = evaluate(baseBatch(), patch(transforms), baseSchema)
      assert((0 until baseSchema.length()).map(i => ints(result.getChild(i))) == Seq(
        Seq(1, 2),
        Seq(10, 20),
        Seq(100, 200)))
      result.close()
    }
  }

  Seq[(String, StructType, StructPatch, DataType)](
    (
      "non-struct output",
      baseSchema,
      patch(),
      IntegerType.INTEGER),
    (
      "too few output fields",
      baseSchema,
      patch(),
      new StructType().add("a", IntegerType.INTEGER).add("b", IntegerType.INTEGER)),
    (
      "too many output fields",
      baseSchema,
      patch(),
      new StructType()
        .add("a", IntegerType.INTEGER)
        .add("b", IntegerType.INTEGER)
        .add("c", IntegerType.INTEGER)
        .add("d", IntegerType.INTEGER)),
    (
      "retained field type mismatch",
      baseSchema,
      patch(),
      new StructType()
        .add("a", StringType.STRING)
        .add("b", IntegerType.INTEGER)
        .add("c", IntegerType.INTEGER)),
    (
      "inserted field type mismatch",
      baseSchema,
      patch(prepended = Seq(Literal.ofString("bad"))),
      new StructType()
        .add("pre", IntegerType.INTEGER)
        .add("a", IntegerType.INTEGER)
        .add("b", IntegerType.INTEGER)
        .add("c", IntegerType.INTEGER)),
    (
      "required missing field",
      baseSchema, {
        val fields = new util.LinkedHashMap[String, StructPatch.FieldTransform]()
        fields.put("missing", fieldTransform(true))
        patch(fields)
      },
      baseSchema),
    (
      "missing input path",
      baseSchema,
      patch(inputPath = Some(new Column("missing"))),
      baseSchema),
    (
      "non-struct input path",
      baseSchema,
      patch(inputPath = Some(new Column("a"))),
      baseSchema)).foreach { case (name, inputSchema, expression, outputType) =>
    test(s"invalid struct patch contract: $name") {
      intercept[RuntimeException] {
        new DefaultExpressionEvaluator(inputSchema, expression, outputType)
      }
    }
  }
}
