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

import io.delta.kernel.types.{IntegerType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class ParseJsonSuite extends AnyFunSuite {
  test("exposes one immutable child and its output schema") {
    val child = new Column("json")
    val schema = new StructType().add("value", IntegerType.INTEGER)
    val expression = new ParseJson(child, schema)

    assert(expression.getJsonExpression eq child)
    assert(expression.getOutputSchema eq schema)
    assert(expression.getChildren.size() == 1)
    assert(expression.getChildren.get(0) eq child)
    assert(expression.toString.contains("PARSE_JSON"))
    intercept[UnsupportedOperationException] {
      expression.getChildren.clear()
    }
  }

  Seq[(String, () => ParseJson)](
    "null JSON expression" -> (() => new ParseJson(null, new StructType())),
    "null output schema" -> (() => new ParseJson(new Column("json"), null))).foreach {
    case (name, build) =>
      test(s"rejects $name") {
        intercept[NullPointerException](build())
      }
  }
}
