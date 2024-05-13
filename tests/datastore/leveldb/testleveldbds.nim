import std/options
import std/os
import std/sequtils
from std/algorithm import sort, reversed

import pkg/asynctest
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore
import pkg/datastore/key
import pkg/datastore/leveldb/leveldbds

import ../dscommontests
import ../modifycommontests
import ../querycommontests

suite "Test Basic LevelDbDatastore":
  let
    tempDir = getTempDir() / "testleveldbds"
    ds = LevelDbDatastore.new(tempDir).tryGet()
    key = Key.init("a:b/c/d:e").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  setupAll:
    createdir(tempDir)

  teardownAll:
    (await ds.close()).tryGet()
    removeDir(tempDir)

  basicStoreTests(ds, key, bytes, otherBytes)
  modifyTests(ds, key)

suite "Test LevelDB Query":
  let tempDir = getTempDir() / "testleveldbds"
  var ds: LevelDbDatastore

  setupAll:
    createdir(tempDir)

  teardownAll:
    removeDir(tempDir)

  setup:
    ds = LevelDbDatastore.new(tempDir).tryGet()

  teardown:
    (await ds.close()).tryGet

  queryTests(ds)
