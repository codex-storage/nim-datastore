import std/options
import std/os
import std/sequtils
from std/algorithm import sort, reversed

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils

import pkg/datastore
import pkg/datastore/key
import pkg/datastore/leveldb/leveldbds

import ../dscommontests
import ../modifycommontests
import ../querycommontests
import ../typeddscommontests

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
  typedDsTests(ds, key)

suite "Test LevelDB Query":
  let tempDir = getTempDir() / "testleveldbds"
  var ds: LevelDbDatastore

  setup:
    createdir(tempDir)
    ds = LevelDbDatastore.new(tempDir).tryGet()

  teardown:
    (await ds.close()).tryGet
    removeDir(tempDir)

  queryTests(ds,
    testLimitsAndOffsets = true,
    testSortOrder = false
  )

suite "Test LevelDB Typed Query":
  let tempDir = getTempDir() / "testleveldbds"
  var ds: LevelDbDatastore

  setup:
    createdir(tempDir)
    ds = LevelDbDatastore.new(tempDir).tryGet()

  teardown:
    (await ds.close()).tryGet
    removeDir(tempDir)

  test "Typed Queries":
    typedDsQueryTests(ds)

suite "LevelDB Query: keys should disregard trailing wildcards":
  let tempDir = getTempDir() / "testleveldbds"
  var
    ds: LevelDbDatastore
    key1: Key
    key2: Key
    key3: Key
    val1: seq[byte]
    val2: seq[byte]
    val3: seq[byte]

  setupAll:
    key1 = Key.init("/a").tryGet
    key2 = Key.init("/a/b").tryGet
    key3 = Key.init("/a/b/c").tryGet
    val1 = "value for 1".toBytes
    val2 = "value for 2".toBytes
    val3 = "value for 3".toBytes

  setup:
    createdir(tempDir)
    ds = LevelDbDatastore.new(tempDir).tryGet()
    (await ds.put(key1, val1)).tryGet
    (await ds.put(key2, val2)).tryGet
    (await ds.put(key3, val3)).tryGet

  teardown:
    (await ds.close()).tryGet
    removeDir(tempDir)

  test "Forward":
    let
      q = Query.init(Key.init("/a/*").tryGet)
      iter = (await ds.query(q)).tryGet
      res = (await allFinished(toSeq(iter)))
        .mapIt( it.read.tryGet )
        .filterIt( it.key.isSome )

    check:
      res.len == 3
      res[0].key.get == key1
      res[0].data == val1

      res[1].key.get == key2
      res[1].data == val2

      res[2].key.get == key3
      res[2].data == val3

    (await iter.dispose()).tryGet

  test "Backwards":
    let
      q = Query.init(Key.init("/a\\*").tryGet)
      iter = (await ds.query(q)).tryGet
      res = (await allFinished(toSeq(iter)))
        .mapIt( it.read.tryGet )
        .filterIt( it.key.isSome )

    check:
      res.len == 3
      res[0].key.get == key1
      res[0].data == val1

      res[1].key.get == key2
      res[1].data == val2

      res[2].key.get == key3
      res[2].data == val3

    (await iter.dispose()).tryGet
