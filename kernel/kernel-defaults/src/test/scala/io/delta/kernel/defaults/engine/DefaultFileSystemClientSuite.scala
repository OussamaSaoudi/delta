/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.engine

import java.io.FileNotFoundException
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.defaults.utils.TestUtils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

class DefaultFileSystemClientSuite extends AnyFunSuite with TestUtils {

  val fsClient = defaultEngine.getFileSystemClient
  val fs = FileSystem.get(configuration)

  private def writeFile(path: String, content: String): Unit = {
    writeBytes(path, content.getBytes("UTF-8"))
  }

  private def writeBytes(path: String, content: Array[Byte]): Unit = {
    val out = fs.create(new Path(path))
    try {
      out.write(content)
    } finally {
      out.close()
    }
  }

  private def readFile(path: String): String = {
    new String(readBytes(path), "UTF-8")
  }

  private def readBytes(path: String): Array[Byte] = {
    val fileStatus = fs.getFileStatus(new Path(path))
    val buffer = new Array[Byte](fileStatus.getLen.toInt)
    val in = fs.open(new Path(path))
    try {
      in.readFully(buffer)
      buffer
    } finally {
      in.close()
    }
  }

  private def withTempSrcAndDestFiles(f: (String, String) => Unit): Unit = {
    withTempDir { tempDir =>
      val src = tempDir + "/source.txt"
      val dest = tempDir + "/dest.txt"
      f(src, dest)
    }
  }

  private def copyTempFiles(dest: String): Seq[String] = {
    fs.listStatus(new Path(dest).getParent)
      .map(_.getPath.getName)
      .filter(name => name.startsWith(".dest.txt.") && name.endsWith(".tmp"))
      .toSeq
  }

  private def recursivelyListedPaths(path: String): Seq[String] = {
    val listed = fsClient.listFromRecursively(path)
    val paths = new ArrayBuffer[String]()
    try {
      listed.forEachRemaining(status => paths += status.getPath)
      paths.toSeq
    } finally {
      listed.close()
    }
  }

  test("list from file") {
    val basePath = fsClient.resolvePath(getTestResourceFilePath("json-files"))
    val listFrom = fsClient.resolvePath(getTestResourceFilePath("json-files/2.json"))

    val actListOutput = new ArrayBuffer[String]()
    val files = fsClient.listFrom(listFrom)
    try {
      fsClient.listFrom(listFrom).forEach(f => actListOutput += f.getPath)
    } finally if (files != null) {
        files.close()
      }

    val expListOutput = Seq(basePath + "/2.json", basePath + "/3.json")

    assert(expListOutput === actListOutput)
  }

  test("list from non-existent file") {
    intercept[FileNotFoundException] {
      fsClient.listFrom("file:/non-existentfileTable/01.json")
    }
  }

  test("recursive listing is exclusive, recursive, and sorted by full path") {
    withTempDir { tempDir =>
      val root = tempDir + "/listing"
      writeFile(root + "/a.json", "a")
      writeFile(root + "/nested/b.json", "b")
      writeFile(root + "/z.json", "z")

      val qualifiedRoot = fsClient.resolvePath(root)
      val listed = recursivelyListedPaths(fsClient.resolvePath(root + "/a.json"))

      assert(listed === Seq(
        qualifiedRoot + "/nested/b.json",
        qualifiedRoot + "/z.json"))
    }
  }

  test("recursive listing treats a trailing slash as a directory") {
    withTempDir { tempDir =>
      val root = tempDir + "/directory"
      writeFile(root + "/a.json", "a")
      writeFile(root + "/nested/b.json", "b")

      val qualifiedRoot = fsClient.resolvePath(root)
      assert(recursivelyListedPaths(qualifiedRoot + "/") === Seq(
        qualifiedRoot + "/a.json",
        qualifiedRoot + "/nested/b.json"))
    }
  }

  test("recursive listing uses unsigned UTF-8 path ordering") {
    withTempDir { tempDir =>
      val root = tempDir + "/unicode"
      val bmpName = "\uE000.json"
      val supplementaryName = "\uD800\uDC00.json"
      writeFile(root + "/" + supplementaryName, "supplementary")
      writeFile(root + "/" + bmpName, "bmp")

      val qualifiedRoot = fsClient.resolvePath(root)
      assert(recursivelyListedPaths(qualifiedRoot + "/") === Seq(
        qualifiedRoot + "/" + bmpName,
        qualifiedRoot + "/" + supplementaryName))
    }
  }

  test("recursive listing supports a missing offset but not a missing directory") {
    withTempDir { tempDir =>
      val root = tempDir + "/offset"
      writeFile(root + "/b.json", "b")
      val qualifiedRoot = fsClient.resolvePath(root)

      assert(recursivelyListedPaths(qualifiedRoot + "/a.json") ===
        Seq(qualifiedRoot + "/b.json"))
      intercept[FileNotFoundException] {
        recursivelyListedPaths(qualifiedRoot + "/missing/")
      }
    }
  }

  test("resolve path") {
    val inputPath = getTestResourceFilePath("json-files")
    val resolvedPath = fsClient.resolvePath(inputPath)

    assert("file:" + inputPath === resolvedPath)
  }

  test("resolve path on non-existent file") {
    val inputPath = "/non-existentfileTable/01.json"
    val resolvedPath = fsClient.resolvePath(inputPath)
    assert("file:" + inputPath === resolvedPath)
  }

  test("mkdirs") {
    withTempDir { tempdir =>
      val dir1 = tempdir + "/test"
      assert(fsClient.mkdirs(dir1))
      assert(fs.exists(new Path(dir1)))

      val dir2 = tempdir + "/test1/test2" // nested
      assert(fsClient.mkdirs(dir2))
      assert(fs.exists(new Path(dir2)))

      val dir3 = "/non-existentfileTable/sfdsd"
      assert(!fsClient.mkdirs(dir3))
      assert(!fs.exists(new Path(dir3)))
    }
  }

  test("getFileStatus") {
    val filePath = getTestResourceFilePath("json-files/1.json")
    val fileStatus = fsClient.getFileStatus(filePath)

    assert(fileStatus.getPath == fsClient.resolvePath(filePath))
    assert(fileStatus.getSize > 0)
    assert(fileStatus.getModificationTime > 0)
  }

  test("getFileStatus on non-existent file") {
    intercept[FileNotFoundException] {
      fsClient.getFileStatus("/non-existent-file.json")
    }
  }

  test("writeBytes honors overwrite semantics") {
    withTempDir { tempDir =>
      val path = tempDir + "/bytes.bin"

      fsClient.writeBytes(path, "first".getBytes("UTF-8"), false)
      assert(readFile(path) == "first")

      intercept[java.nio.file.FileAlreadyExistsException] {
        fsClient.writeBytes(path, "rejected".getBytes("UTF-8"), false)
      }
      assert(readFile(path) == "first")

      fsClient.writeBytes(path, "second".getBytes("UTF-8"), true)
      assert(readFile(path) == "second")
    }
  }

  test("copyFileAtomically - overwrite=false, dest does not exist") {
    withTempSrcAndDestFiles { (src, dest) =>
      writeFile(src, "test content")
      fsClient.copyFileAtomically(src, dest, false /* overwrite */ )

      assert(fs.exists(new Path(dest)))
      assert(readFile(dest) == "test content")
      assert(readFile(src) == "test content")
    }
  }

  test("copyFileAtomically preserves empty, newline, and arbitrary binary contents exactly") {
    val contents = Seq(
      Array.emptyByteArray,
      "no trailing newline".getBytes("UTF-8"),
      "one trailing newline\n".getBytes("UTF-8"),
      "embedded\nnewlines\r\nwithout a final newline".getBytes("UTF-8"),
      Array[Byte](0, 10, 13, -1, 42, 0, 10))

    contents.foreach { content =>
      withTempSrcAndDestFiles { (src, dest) =>
        writeBytes(src, content)

        fsClient.copyFileAtomically(src, dest, false /* overwrite */ )

        assert(readBytes(dest).sameElements(content))
        assert(readBytes(src).sameElements(content))
      }
    }
  }

  test("copyFileAtomically - overwrite=false, dest exists") {
    withTempSrcAndDestFiles { (src, dest) =>
      writeFile(src, "source content")
      writeFile(dest, "existing content")

      intercept[java.nio.file.FileAlreadyExistsException] {
        fsClient.copyFileAtomically(src, dest, false /* overwrite */ )
      }
      assert(readFile(dest) == "existing content")
      assert(readFile(src) == "source content")
      assert(copyTempFiles(dest).isEmpty)
    }
  }

  test("copyFileAtomically - overwrite=true") {
    withTempSrcAndDestFiles { (src, dest) =>
      val content = Array[Byte](0, 10, -1, 13, 0)
      writeBytes(src, content)
      writeBytes(dest, "old content".getBytes("UTF-8"))

      fsClient.copyFileAtomically(src, dest, true /* overwrite */ )
      assert(readBytes(dest).sameElements(content))
      assert(readBytes(src).sameElements(content))
      assert(copyTempFiles(dest).isEmpty)
    }
  }

  test("copyFileAtomically handles source and destination resolving to the same path") {
    withTempSrcAndDestFiles { (src, _) =>
      val content = Array[Byte](0, 1, 10, -1)
      writeBytes(src, content)

      fsClient.copyFileAtomically(src, fsClient.resolvePath(src), true /* overwrite */ )
      assert(readBytes(src).sameElements(content))

      intercept[java.nio.file.FileAlreadyExistsException] {
        fsClient.copyFileAtomically(src, src, false /* overwrite */ )
      }
      assert(readBytes(src).sameElements(content))
    }
  }

  test("copyFileAtomically serializes concurrent create-if-absent copies") {
    withTempSrcAndDestFiles { (src, dest) =>
      val content = Array.tabulate[Byte](32768)(index => (index % 251).toByte)
      writeBytes(src, content)
      val taskCount = 8
      val ready = new CountDownLatch(taskCount)
      val start = new CountDownLatch(1)
      val executor = Executors.newFixedThreadPool(taskCount)
      try {
        val futures = (0 until taskCount).map { _ =>
          executor.submit(new Callable[Option[Throwable]] {
            override def call(): Option[Throwable] = {
              ready.countDown()
              start.await()
              try {
                fsClient.copyFileAtomically(src, dest, false /* overwrite */ )
                None
              } catch {
                case failure: Throwable => Some(failure)
              }
            }
          })
        }
        assert(ready.await(30, TimeUnit.SECONDS))
        start.countDown()
        val results = futures.map(_.get(30, TimeUnit.SECONDS))

        assert(results.count(_.isEmpty) == 1)
        assert(results.flatten.size == taskCount - 1)
        assert(results.flatten.forall(_.isInstanceOf[java.nio.file.FileAlreadyExistsException]))
        assert(readBytes(dest).sameElements(content))
        assert(readBytes(src).sameElements(content))
        assert(copyTempFiles(dest).isEmpty)
      } finally {
        executor.shutdownNow()
      }
    }
  }

  test("copyFileAtomically removes its temp file after a publish failure") {
    withTempSrcAndDestFiles { (src, dest) =>
      writeFile(src, "source content")
      fs.mkdirs(new Path(dest))
      writeFile(dest + "/child", "keep directory non-empty")

      intercept[java.io.IOException] {
        fsClient.copyFileAtomically(src, dest, true /* overwrite */ )
      }

      assert(fs.getFileStatus(new Path(dest)).isDirectory)
      assert(readFile(dest + "/child") == "keep directory non-empty")
      assert(readFile(src) == "source content")
      assert(copyTempFiles(dest).isEmpty)
    }
  }

  test("copyFileAtomically rejects a destination without a parent before writing") {
    withTempSrcAndDestFiles { (src, _) =>
      writeFile(src, "source content")

      val error = intercept[java.io.IOException] {
        fsClient.copyFileAtomically(src, "file:/", true /* overwrite */ )
      }

      assert(error.getMessage.contains("Cannot determine parent directory"))
      assert(readFile(src) == "source content")
    }
  }

  test("copyFileAtomically with non-existent source") {
    withTempSrcAndDestFiles { (src, dest) =>
      intercept[FileNotFoundException] {
        fsClient.copyFileAtomically(src, dest, false /* overwrite */ )
      }
      assert(!fs.exists(new Path(dest)))
      assert(copyTempFiles(dest).isEmpty)
    }
  }
}
