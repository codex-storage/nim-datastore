import std/options
import std/sequtils
import std/os
from std/algorithm import sort, reversed

import pkg/asynctest
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore/memoryds

import ./dscommontests
import ./querycommontests

suite "Test Basic MemoryDatastore":
  let
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  var
    memStore: MemoryDatastore

  setupAll:
    memStore = MemoryDatastore.new()

  basicStoreTests(memStore, key, bytes, otherBytes)

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

    ds = MemoryDatastore.new()

  teardown:

    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  queryTests(ds, false)
