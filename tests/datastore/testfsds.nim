import std/algorithm
import std/options
import std/os

import pkg/asynctest/unittest2
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore/fsds

import ./basictests

suite "Test Basic FSDatastore":
  let
    (path, _, _) = instantiationInfo(-1, fullPaths = true) # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  var
    fsStore: FSDatastore

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    fsStore = FSDatastore.new(root = basePathAbs).tryGet()

  teardownAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  basicStoreTests(fsStore, key, bytes, otherBytes)

suite "Test Misc FSDatastore":
  let
    (path, _, _) = instantiationInfo(-1, fullPaths = true) # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    bytes = "some bytes".toBytes

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

  teardownAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  test "Test validDepth()":
    let
      fs = FSDatastore.new(root = "/", depth = 3).tryGet()
      invalid = Key.init("/a/b/c/d").tryGet()
      valid = Key.init("/a/b/c").tryGet()

    check:
      not fs.validDepth(invalid)
      fs.validDepth(valid)

  test "Test invalid key (path) depth":
    let
      fs = FSDatastore.new(root = basePathAbs, depth = 3).tryGet()
      key = Key.init("/a/b/c/d").tryGet()

    check:
      (await fs.put(key, bytes)).isErr
      (await fs.get(key)).isErr
      (await fs.delete(key)).isErr
      (await fs.contains(key)).isErr

  test "Test valid key (path) depth":
    let
      fs = FSDatastore.new(root = basePathAbs, depth = 3).tryGet()
      key = Key.init("/a/b/c").tryGet()

    check:
      (await fs.put(key, bytes)).isOk
      (await fs.get(key)).isOk
      (await fs.delete(key)).isOk
      (await fs.contains(key)).isOk
