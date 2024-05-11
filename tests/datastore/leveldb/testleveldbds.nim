import std/options
import std/os
import std/sequtils
from std/algorithm import sort, reversed

import pkg/asynctest
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore/sql/sqliteds

import ../dscommontests
import ../modifycommontests
import ../querycommontests

suite "Test Basic LevelDBDatastore":
  let
    tempDir = getTempDir() / "testleveldbds"
    ds = LevelDBDatastore.new(tempDir).tryGet()
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
  var ds: LevelDBDatastore

  setupAll:
    createdir(tempDir)

  teardownAll:
    removeDir(tempDir)

  setup:
    ds = LevelDBDatastore.new(tempDir).tryGet()

  teardown:
    (await ds.close()).tryGet

  queryTests(ds)
