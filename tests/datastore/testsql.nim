import std/options
import std/os
import std/sequtils
from std/algorithm import sort, reversed

import pkg/asynctest
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore/sql

import ./dscommontests
import ./querycommontests

suite "Test Basic SQLiteDatastore":
  let
    ds = SQLiteDatastore.new(Memory).tryGet()
    key = Key.init("a:b/c/d:e").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  teardownAll:
    (await ds.close()).tryGet()

  basicStoreTests(ds, key, bytes, otherBytes)

suite "Test Read Only SQLiteDatastore":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    filename = "test_store" & DbExt
    dbPathAbs = basePathAbs / filename
    key = Key.init("a:b/c/d:e").tryGet()
    bytes = "some bytes".toBytes

  var
    dsDb: SQLiteDatastore
    readOnlyDb: SQLiteDatastore

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    dsDb = SQLiteDatastore.new(path = dbPathAbs).tryGet()
    readOnlyDb = SQLiteDatastore.new(path = dbPathAbs, readOnly = true).tryGet()

  teardownAll:
    (await dsDb.close()).tryGet()
    (await readOnlyDb.close()).tryGet()

    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  test "put":
    check:
      (await readOnlyDb.put(key, bytes)).isErr

    (await dsDb.put(key, bytes)).tryGet()

  test "get":
    check:
      (await readOnlyDb.get(key)).tryGet() == bytes
      (await dsDb.get(key)).tryGet() == bytes

  test "delete":
    check:
      (await readOnlyDb.delete(key)).isErr

    (await dsDb.delete(key)).tryGet()

  test "contains":
    check:
      not (await readOnlyDb.has(key)).tryGet()
      not (await dsDb.has(key)).tryGet()

# suite "Test Query":
#   var
#     ds: SQLiteDatastore

#   setup:
#     ds = SQLiteDatastore.new(Memory).tryGet()

#   teardown:
#     (await ds.close()).tryGet

#   queryTests(ds)
