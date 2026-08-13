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
package io.delta.kernel.deletionvectors

import java.io.ByteArrayInputStream
import java.util.Optional

import io.delta.kernel.engine.{FileReadRequest, FileSystemClient}
import io.delta.kernel.internal.actions.DeletionVectorDescriptor
import io.delta.kernel.internal.deletionvectors.DeletionVectorStoredBitmap
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.test.BaseMockFileSystemClient
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class DeletionVectorStoredBitmapSuite extends AnyFunSuite {
  test("on-disk deletion vector without an offset starts after the format header") {
    var request: FileReadRequest = null
    val fileSystemClient: FileSystemClient = new BaseMockFileSystemClient {
      override def readFiles(
          requests: CloseableIterator[FileReadRequest])
          : CloseableIterator[ByteArrayInputStream] = {
        request = requests.next()
        Utils.singletonCloseableIterator(new ByteArrayInputStream(Array.emptyByteArray))
      }
    }
    val descriptor = new DeletionVectorDescriptor(
      DeletionVectorDescriptor.PATH_DV_MARKER,
      "file:///table/dv.bin",
      Optional.empty(),
      1,
      1)

    intercept[Exception] {
      new DeletionVectorStoredBitmap(descriptor, Optional.of("file:///table"))
        .load(fileSystemClient)
    }

    assert(request.getStartOffset === 1)
  }

  test("on-disk deletion vector preserves an explicit offset") {
    var request: FileReadRequest = null
    val fileSystemClient: FileSystemClient = new BaseMockFileSystemClient {
      override def readFiles(
          requests: CloseableIterator[FileReadRequest])
          : CloseableIterator[ByteArrayInputStream] = {
        request = requests.next()
        Utils.singletonCloseableIterator(new ByteArrayInputStream(Array.emptyByteArray))
      }
    }
    val descriptor = new DeletionVectorDescriptor(
      DeletionVectorDescriptor.PATH_DV_MARKER,
      "file:///table/dv.bin",
      Optional.of(17),
      1,
      1)

    intercept[Exception] {
      new DeletionVectorStoredBitmap(descriptor, Optional.of("file:///table"))
        .load(fileSystemClient)
    }

    assert(request.getStartOffset === 17)
  }
}
