import std/options
import std/sequtils
import std/os
from std/algorithm import sort, reversed

import pkg/asynctest/unittest2
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore/fsds

import ./dscommontests
import ./querycommontests

suite "Test Basic MemoryDatastore":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  var
    fsStore: MemoryDatastore

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    fsStore = MemoryDatastore.new(root = basePathAbs, depth = 3).tryGet()

  teardownAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  basicStoreTests(fsStore, key, bytes, otherBytes)

suite "Test Misc MemoryDatastore":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    bytes = "some bytes".toBytes

  setup:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

  teardown:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  test "Test validDepth()":
    let
      fs = MemoryDatastore.new(root = "/", depth = 3).tryGet()
      invalid = Key.init("/a/b/c/d").tryGet()
      valid = Key.init("/a/b/c").tryGet()

    check:
      not fs.validDepth(invalid)
      fs.validDepth(valid)

  test "Test invalid key (path) depth":
    let
      fs = MemoryDatastore.new(root = basePathAbs, depth = 3).tryGet()
      key = Key.init("/a/b/c/d").tryGet()

    check:
      (await fs.put(key, bytes)).isErr
      (await fs.get(key)).isErr
      (await fs.delete(key)).isErr
      (await fs.has(key)).isErr

  test "Test valid key (path) depth":
    let
      fs = MemoryDatastore.new(root = basePathAbs, depth = 3).tryGet()
      key = Key.init("/a/b/c").tryGet()

    check:
      (await fs.put(key, bytes)).isOk
      (await fs.get(key)).isOk
      (await fs.delete(key)).isOk
      (await fs.has(key)).isOk

  test "Test key cannot write outside of root":
    let
      fs = MemoryDatastore.new(root = basePathAbs, depth = 3).tryGet()
      key = Key.init("/a/../../c").tryGet()

    check:
      (await fs.put(key, bytes)).isErr
      (await fs.get(key)).isErr
      (await fs.delete(key)).isErr
      (await fs.has(key)).isErr

  test "Test key cannot convert to invalid path":
    let
      fs = MemoryDatastore.new(root = basePathAbs).tryGet()

    for c in invalidFilenameChars:
      if c == ':': continue
      if c == '/': continue

      let
        key = Key.init("/" & c).tryGet()

      check:
        (await fs.put(key, bytes)).isErr
        (await fs.get(key)).isErr
        (await fs.delete(key)).isErr
        (await fs.has(key)).isErr

suite "Test Query":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath

  var
    ds: MemoryDatastore

  setup:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    ds = MemoryDatastore.new(root = basePathAbs, depth = 5).tryGet()

  teardown:

    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  queryTests(ds, false)
