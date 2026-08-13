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

import org.scalatest.funsuite.AnyFunSuite

class StructExpressionSuite extends AnyFunSuite {
  test("field expressions are immutable and preserve ordinal order") {
    val fields = new util.ArrayList[Expression]()
    fields.add(Literal.ofInt(1))
    fields.add(Literal.ofString("value"))

    val expression = new StructExpression(fields, Literal.ofBoolean(true))
    fields.clear()

    assert(expression.getFieldExpressions.size() == 2)
    assert(expression.getFieldExpressions.get(0) == Literal.ofInt(1))
    assert(expression.getChildren.size() == 3)
    assert(expression.getChildren.get(2) == Literal.ofBoolean(true))
    assert(expression.getNullabilityPredicate.isPresent)
    intercept[UnsupportedOperationException] {
      expression.getFieldExpressions.add(Literal.ofLong(2))
    }
  }

  test("plain struct has no nullability predicate") {
    val expression = new StructExpression(util.Collections.singletonList(Literal.ofInt(1)))
    assert(!expression.getNullabilityPredicate.isPresent)
    assert(expression.getChildren == expression.getFieldExpressions)
  }

  Seq[(String, () => StructExpression)](
    "null field list" -> (() => new StructExpression(null.asInstanceOf[util.List[Expression]])),
    "null field" -> (() =>
      new StructExpression(util.Collections.singletonList(null.asInstanceOf[Expression]))),
    "null predicate" -> (() =>
      new StructExpression(
        util.Collections.singletonList(Literal.ofInt(1)),
        null.asInstanceOf[Expression]))).foreach { case (name, build) =>
    test(s"invalid struct expression: $name") {
      intercept[NullPointerException](build())
    }
  }
}
