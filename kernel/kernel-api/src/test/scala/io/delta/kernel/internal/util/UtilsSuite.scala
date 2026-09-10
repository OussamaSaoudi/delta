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
package io.delta.kernel.internal.util

import org.scalatest.funsuite.AnyFunSuite

class UtilsSuite extends AnyFunSuite {
  test("closeCloseables attempts every close after an Error and suppresses later failures") {
    val first = new AssertionError("first")
    val second = new IllegalStateException("second")
    var finalCloseCalled = false

    val thrown = intercept[AssertionError] {
      Utils.closeCloseables(
        new AutoCloseable {
          override def close(): Unit = throw first
        },
        new AutoCloseable {
          override def close(): Unit = throw second
        },
        new AutoCloseable {
          override def close(): Unit = finalCloseCalled = true
        })
    }

    assert(thrown eq first)
    assert(thrown.getSuppressed.toSeq === Seq(second))
    assert(finalCloseCalled)
  }
}
