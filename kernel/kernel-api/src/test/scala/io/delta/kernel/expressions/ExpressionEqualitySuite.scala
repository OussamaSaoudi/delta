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
package io.delta.kernel.expressions

import java.util

import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class ExpressionEqualitySuite extends AnyFunSuite {
  private final class TestOpaqueExpression(
      key: String,
      children: util.List[Expression])
      extends OpaqueExpression(children) {
    override protected def semanticKey(): AnyRef = key
  }

  private final class OtherOpaqueExpression(
      key: String,
      children: util.List[Expression])
      extends OpaqueExpression(children) {
    override protected def semanticKey(): AnyRef = key
  }

  private final class TestOpaquePredicate(
      key: String,
      children: util.List[Expression])
      extends OpaquePredicate(children) {
    override protected def semanticKey(): AnyRef = key
  }

  test("scalar and predicate equality is structural") {
    val scalar = new ScalarExpression(
      "to_json",
      util.Collections.singletonList[Expression](new Column("payload")))
    val sameScalar = new ScalarExpression(
      "TO_JSON",
      util.Collections.singletonList[Expression](new Column("payload")))
    assertEqualAndSameHash(scalar, sameScalar)
    assert(scalar != new ScalarExpression("TO_JSON", util.Collections.emptyList()))

    val left = new Predicate("=", Literal.ofString("Aa"), Literal.ofInt(1))
    val right = new Predicate("=", Literal.ofString("BB"), Literal.ofInt(1))
    assert(left.toString.hashCode == right.toString.hashCode)
    assert(left != right)

    val in = new In(
      new Column("id"),
      util.Arrays.asList[Expression](Literal.ofInt(1), Literal.ofInt(2)))
    val sameIn = new In(
      new Column("id"),
      util.Arrays.asList[Expression](Literal.ofInt(1), Literal.ofInt(2)))
    assertEqualAndSameHash(in, sameIn)

    val collated = new Predicate(
      "=",
      Literal.ofString("a"),
      Literal.ofString("A"),
      CollationIdentifier.fromString("SPARK.UTF8_LCASE"))
    val sameCollated = new Predicate(
      "=",
      Literal.ofString("a"),
      Literal.ofString("A"),
      CollationIdentifier.fromString("spark.utf8_lcase"))
    assertEqualAndSameHash(collated, sameCollated)
  }

  test("composite expression payloads are structural") {
    val schema = new StructType().add("id", IntegerType.INTEGER)
    assertEqualAndSameHash(
      new ParseJson(new Column("json"), schema),
      new ParseJson(new Column("json"), schema))
    assertEqualAndSameHash(
      new MapToStruct(new Column("map")),
      new MapToStruct(new Column("map")))
    assertEqualAndSameHash(
      new PartitionValueExpression(new Column("part"), IntegerType.INTEGER),
      new PartitionValueExpression(new Column("part"), IntegerType.INTEGER))
    assertEqualAndSameHash(
      new StructExpression(
        util.Collections.singletonList[Expression](new Column("id")),
        new Predicate("IS_NOT_NULL", new Column("id"))),
      new StructExpression(
        util.Collections.singletonList[Expression](new Column("id")),
        new Predicate("IS_NOT_NULL", new Column("id"))))

    val patch = structPatch(replace = true)
    assertEqualAndSameHash(patch, structPatch(replace = true))
    assert(patch != structPatch(replace = false))
    assertEqualAndSameHash(new UnknownExpression("future"), new UnknownExpression("future"))
    assertEqualAndSameHash(new UnknownPredicate("future"), new UnknownPredicate("future"))
  }

  test("nested literal values are structural") {
    val arrayType = new ArrayType(FloatType.FLOAT, false)
    val array = Literal.ofArray(
      VectorUtils.buildArrayValue(util.Arrays.asList[Float](1.0f, 2.0f), FloatType.FLOAT),
      arrayType)
    val sameArray = Literal.ofArray(
      VectorUtils.buildArrayValue(util.Arrays.asList[Float](1.0f, 2.0f), FloatType.FLOAT),
      arrayType)
    assertEqualAndSameHash(array, sameArray)

    val mapType = new MapType(StringType.STRING, IntegerType.INTEGER, false)
    val map = Literal.ofMap(
      VectorUtils.buildMapValue(util.Arrays.asList("a"), util.Arrays.asList(1), mapType),
      mapType)
    val sameMap = Literal.ofMap(
      VectorUtils.buildMapValue(util.Arrays.asList("a"), util.Arrays.asList(1), mapType),
      mapType)
    assertEqualAndSameHash(map, sameMap)

    val structType = new StructType().add("bytes", BinaryType.BINARY)
    val row = GenericRow.fromValues(
      structType,
      util.Collections.singletonList[AnyRef](Array[Byte](1, 2)))
    val sameRow = GenericRow.fromValues(
      structType,
      util.Collections.singletonList[AnyRef](Array[Byte](1, 2)))
    assertEqualAndSameHash(
      Literal.ofStruct(row, structType),
      Literal.ofStruct(sameRow, structType))
  }

  test("opaque equality includes exact subclass, semantic key, and children") {
    val children = util.Collections.singletonList[Expression](new Column("id"))
    assertEqualAndSameHash(
      new TestOpaqueExpression("key", children),
      new TestOpaqueExpression("key", children))
    assert(new TestOpaqueExpression("key", children) !=
      new TestOpaqueExpression("other", children))
    assert(new TestOpaqueExpression("key", children) !=
      new OtherOpaqueExpression("key", children))
    assertEqualAndSameHash(
      new TestOpaquePredicate("key", children),
      new TestOpaquePredicate("key", children))
  }

  private def structPatch(replace: Boolean): StructPatch = {
    val transforms = new util.LinkedHashMap[String, StructPatch.FieldTransform]()
    transforms.put(
      "id",
      new StructPatch.FieldTransform(
        util.Collections.singletonList[Expression](Literal.ofInt(1)),
        replace,
        false))
    new StructPatch(
      util.Optional.of(new Column("nested")),
      transforms,
      util.Collections.singletonList[Expression](Literal.ofString("before")),
      util.Collections.singletonList[Expression](Literal.ofString("after")))
  }

  private def assertEqualAndSameHash(left: Expression, right: Expression): Unit = {
    assert(left == right)
    assert(right == left)
    assert(left.hashCode == right.hashCode)
  }
}
