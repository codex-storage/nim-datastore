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
    createDir(tempDir)

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
    createDir(tempDir)
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
    createDir(tempDir)
    ds = LevelDbDatastore.new(tempDir).tryGet()

  teardown:
    (await ds.close()).tryGet
    removeDir(tempDir)

  test "Typed Queries":
    typedDsQueryTests(ds)

suite "LevelDB Query":
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
    createDir(tempDir)
    ds = LevelDbDatastore.new(tempDir).tryGet()
    (await ds.put(key1, val1)).tryGet
    (await ds.put(key2, val2)).tryGet
    (await ds.put(key3, val3)).tryGet

  teardown:
    (await ds.close()).tryGet
    removeDir(tempDir)

  test "should query by prefix":
    let
      q = Query.init(Key.init("/a").tryGet)
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

  test "should disregard forward trailing wildcards in keys":
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

  test "should disregard backward trailing wildcards in key":
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

  test "should dispose automatically of iterators when finished":
    let
      q = Query.init(Key.init("/a/b/c").tryGet)
      iter = (await ds.query(q)).tryGet

    let val = (await iter.next()).tryGet()
    check val.key.get == key3
    check val.data == val3

    check iter.finished == false
    check iter.disposed == false

    let val2 = (await iter.next()).tryGet()
    check val2.key == Key.none
    check val2.data == EmptyBytes

    check iter.finished == true
    check iter.disposed == true

  test "should dispose automatically of iterators when datastore is closed":
    let
      q1 = Query.init(Key.init("/a/b/c").tryGet)
      q2 = Query.init(Key.init("/a/b").tryGet)
      i1 = (await ds.query(q1)).tryGet
      i2 = (await ds.query(q2)).tryGet

    check i1.disposed == false
    check i2.disposed == false

    (await ds.close()).tryGet

    check i1.disposed == true
    check i2.disposed == true

  test "should have idempotent QueryIterator.dispose":
    let q = Query.init(Key.init("/a/b/c").tryGet)
    let iter = (await ds.query(q)).tryGet
    (await iter.dispose()).tryGet
    (await iter.dispose()).tryGet

  test "should stop tracking iterator objects once those are disposed":
    let q = Query.init(Key.init("/a/b/c").tryGet)
    let iter = (await ds.query(q)).tryGet
    check ds.openIteratorCount == 1
    (await iter.dispose()).tryGet
    check ds.openIteratorCount == 0
