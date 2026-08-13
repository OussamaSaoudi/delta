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
import java.util.Optional

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

class StructPatchSuite extends AnyFunSuite {
  test("patch state is deeply immutable and children preserve expression order") {
    val inserted = new util.ArrayList[Expression](Seq(Literal.ofInt(2)).asJava)
    val transform = new StructPatch.FieldTransform(inserted, false, false)
    val transforms = new util.LinkedHashMap[String, StructPatch.FieldTransform]()
    transforms.put("anchor", transform)
    val prepended = new util.ArrayList[Expression](Seq(Literal.ofInt(1)).asJava)
    val appended = new util.ArrayList[Expression](Seq(Literal.ofInt(3)).asJava)

    val patch = new StructPatch(Optional.empty(), transforms, prepended, appended)
    inserted.clear()
    transforms.clear()
    prepended.clear()
    appended.clear()

    assert(patch.getFieldTransforms.keySet().asScala.toSeq == Seq("anchor"))
    assert(patch.getFieldTransforms.get("anchor").getExpressions.size() == 1)
    assert(patch.getChildren == Seq[Expression](
      Literal.ofInt(1),
      Literal.ofInt(2),
      Literal.ofInt(3)).asJava)
    intercept[UnsupportedOperationException] {
      patch.getFieldTransforms.clear()
    }
    intercept[UnsupportedOperationException] {
      transform.getExpressions.clear()
    }
  }

  Seq[(String, () => Any)](
    "null input path" -> (() =>
      new StructPatch(null, new util.HashMap(), Seq.empty.asJava, Seq.empty.asJava)),
    "null field map" -> (() =>
      new StructPatch(Optional.empty(), null, Seq.empty.asJava, Seq.empty.asJava)),
    "null field name" -> (() => {
      val fields = new util.HashMap[String, StructPatch.FieldTransform]()
      fields.put(null, new StructPatch.FieldTransform(Seq.empty.asJava, true, false))
      new StructPatch(Optional.empty(), fields, Seq.empty.asJava, Seq.empty.asJava)
    }),
    "null field transform" -> (() => {
      val fields = new util.HashMap[String, StructPatch.FieldTransform]()
      fields.put("x", null)
      new StructPatch(Optional.empty(), fields, Seq.empty.asJava, Seq.empty.asJava)
    }),
    "null transform expressions" -> (() =>
      new StructPatch.FieldTransform(null, true, false)),
    "null transform expression" -> (() =>
      new StructPatch.FieldTransform(Seq(null.asInstanceOf[Expression]).asJava, true, false)),
    "null prepended expression" -> (() =>
      new StructPatch(
        Optional.empty(),
        new util.HashMap(),
        Seq(null.asInstanceOf[Expression]).asJava,
        Seq.empty.asJava)),
    "null appended fields" -> (() =>
      new StructPatch(Optional.empty(), new util.HashMap(), Seq.empty.asJava, null))).foreach {
    case (name, build) =>
      test(s"invalid struct patch: $name") {
        intercept[NullPointerException](build())
      }
  }
}
