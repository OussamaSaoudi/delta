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
package io.delta.kernel.defaults.internal.data

import io.delta.kernel.types.{ArrayType, IntegerType, MapType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class StrictJsonRowParserSuite extends AnyFunSuite {
  test("selects the decoder once from the complete schema") {
    val streamingSchema = new StructType().add("value", IntegerType.INTEGER)
    assert(!StrictJsonRowParser.forSchema(streamingSchema).usesTreeDecoder())

    val duplicateStruct = new StructType()
      .add("value", IntegerType.INTEGER)
      .add("value", StringType.STRING)
    val nestedDuplicateSchema = new StructType().add(
      "values",
      new MapType(
        StringType.STRING,
        new ArrayType(duplicateStruct, true),
        true))
    assert(StrictJsonRowParser.forSchema(nestedDuplicateSchema).usesTreeDecoder())
  }
}
