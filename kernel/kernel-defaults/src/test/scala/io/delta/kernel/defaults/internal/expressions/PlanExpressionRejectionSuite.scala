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

import java.util.ArrayList

import scala.jdk.CollectionConverters._

import io.delta.kernel.expressions._
import io.delta.kernel.types.{BooleanType, DataType, IntegerType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class PlanExpressionRejectionSuite extends AnyFunSuite {
  private val emptySchema = new StructType()

  private def assertRejected(
      expression: Expression,
      outputType: DataType,
      expectedName: String): Unit = {
    val error = intercept[UnsupportedOperationException] {
      new DefaultExpressionEvaluator(emptySchema, expression, outputType)
    }
    assert(error.getMessage.contains(expectedName))
    assert(error.getMessage.contains(expression.getClass.getName))
  }

  test("unknown and opaque expressions fail loudly") {
    val cases = Seq[(Expression, DataType, String)](
      (new UnknownExpression("future-expression"), IntegerType.INTEGER, "future-expression"),
      (
        new OpaqueExpression(
          "engine-expression",
          Seq[Expression](Literal.ofInt(1)).asJava),
        IntegerType.INTEGER,
        "engine-expression"),
      (new UnknownPredicate("future-predicate"), BooleanType.BOOLEAN, "future-predicate"),
      (
        new OpaquePredicate(
          "engine-predicate",
          Seq[Expression](Literal.ofInt(1)).asJava),
        BooleanType.BOOLEAN,
        "engine-predicate"))

    cases.foreach { case (expression, outputType, expectedName) =>
      assertRejected(expression, outputType, expectedName)
    }
  }

  test("serialized Rust casts fail as unknown expressions") {
    assertRejected(
      new UnknownExpression("cast_to_integer"),
      IntegerType.INTEGER,
      "cast_to_integer")
  }

  test("opaque nodes defensively copy and expose immutable children") {
    val source = new ArrayList[Expression]()
    source.add(Literal.ofInt(1))

    val expression = new OpaqueExpression("expression", source)
    val predicate = new OpaquePredicate("predicate", source)
    source.add(Literal.ofInt(2))

    assert(expression.getChildren.size() === 1)
    assert(predicate.getChildren.size() === 1)
    intercept[UnsupportedOperationException] {
      expression.getChildren.add(Literal.ofInt(3))
    }
    intercept[UnsupportedOperationException] {
      predicate.getChildren.add(Literal.ofInt(3))
    }
  }

  test("marker nodes reject null names and child lists") {
    intercept[NullPointerException](new UnknownExpression(null))
    intercept[NullPointerException](new UnknownPredicate(null))
    intercept[NullPointerException](new OpaqueExpression(null, Seq.empty[Expression].asJava))
    intercept[NullPointerException](new OpaqueExpression("name", null))
    intercept[NullPointerException](new OpaquePredicate(null, Seq.empty[Expression].asJava))
    intercept[NullPointerException](new OpaquePredicate("name", null))
  }
}
