import std/algorithm
import std/options
import std/os

import pkg/asynctest/unittest2
import pkg/chronos
import pkg/stew/results

import ../../datastore/filesystem_datastore
import ../../datastore/sqlite_datastore
import ../../datastore/tiered_datastore
import ./templates

suite "TieredDatastore":
  # assumes tests/test_all is run from project root, e.g. with `nimble test`
  let
    bytes = @[1.byte, 2.byte, 3.byte]
    key = Key.init("a:b/c/d:e").get
    root = "tests" / "test_data"
    rootAbs = getCurrentDir() / root

  var
    ds1: FileSystemDatastore
    ds2: SQLiteDatastore

  setup:
    removeDir(rootAbs)
    require(not dirExists(rootAbs))
    createDir(rootAbs)
    ds1 = FileSystemDatastore.new(rootAbs).get
    ds2 = SQLiteDatastore.new(memory).get

  teardown:
    removeDir(rootAbs)
    require(not dirExists(rootAbs))
    if not ds2.isNil: await ds2.close
    ds2 = nil

  asyncTest "new":
    check:
      TieredDatastore.new().isErr
      TieredDatastore.new([]).isErr
      TieredDatastore.new(@[]).isErr
      TieredDatastore.new(ds1, ds2).isOk
      TieredDatastore.new([ds1, ds2]).isOk
      TieredDatastore.new(@[ds1, ds2]).isOk

  asyncTest "accessors":
    let
      stores = @[ds1, ds2]

    check:
      TieredDatastore.new(ds1, ds2).get.stores == stores
      TieredDatastore.new([ds1, ds2]).get.stores == stores
      TieredDatastore.new(@[ds1, ds2]).get.stores == stores

  asyncTest "put":
    let
      ds = TieredDatastore.new(ds1, ds2).get

    assert (await ds1.get(key)).get.isNone
    assert (await ds2.get(key)).get.isNone

    let
      putRes = await ds.put(key, bytes)

    check:
      putRes.isOk
      (await ds1.get(key)).get.get == bytes
      (await ds2.get(key)).get.get == bytes

  asyncTest "delete":
    let
      ds = TieredDatastore.new(ds1, ds2).get
      putRes = await ds.put(key, bytes)

    assert putRes.isOk
    assert (await ds1.get(key)).get.get == bytes
    assert (await ds2.get(key)).get.get == bytes

    let
      delRes = await ds.delete(key)

    check:
      delRes.isOk
      (await ds1.get(key)).get.isNone
      (await ds2.get(key)).get.isNone

  asyncTest "contains":
    let
      ds = TieredDatastore.new(ds1, ds2).get

    assert not (await ds1.contains(key)).get
    assert not (await ds2.contains(key)).get

    let
      putRes = await ds.put(key, bytes)

    assert putRes.isOk

    let
      containsRes = await ds.contains(key)

    check:
      containsRes.isOk
      containsRes.get
      (await ds1.contains(key)).get
      (await ds2.contains(key)).get

  asyncTest "get":
    var
      ds = TieredDatastore.new(ds1, ds2).get

    assert (await ds1.get(key)).get.isNone
    assert (await ds2.get(key)).get.isNone

    check: (await ds.get(key)).get.isNone

    let
      putRes = await ds.put(key, bytes)

    assert putRes.isOk

    var
      getRes = await ds.get(key)

    check:
      getRes.isOk
      getRes.get.isSome
      getRes.get.get == bytes
      (await ds1.get(key)).get.isSome
      (await ds2.get(key)).get.isSome
      (await ds1.get(key)).get.get == bytes
      (await ds2.get(key)).get.get == bytes

    removeDir(rootAbs)
    assert (not dirExists(rootAbs))
    createDir(rootAbs)
    ds1 = FileSystemDatastore.new(rootAbs).get
    ds = TieredDatastore.new(ds1, ds2).get

    assert (await ds1.get(key)).get.isNone
    assert (await ds2.get(key)).get.isSome
    assert (await ds2.get(key)).get.get == bytes

    getRes = await ds.get(key)

    check:
      getRes.isOk
      getRes.get.isSome
      getRes.get.get == bytes
      (await ds1.get(key)).get.isSome
      (await ds1.get(key)).get.get == bytes

  asyncTest "query":
    let
      ds = TieredDatastore.new(ds1, ds2).get

      key1 = Key.init("a/b").get
      key2 = Key.init("a/b:c").get
      key3 = Key.init("a/b:c/d").get

      bytes1  = @[1.byte, 2.byte, 3.byte]
      bytes2  = @[4.byte, 5.byte, 6.byte]
      bytes3: seq[byte] = @[]

      queryKey1 = Key.init("a/*").get
      queryKey2 = Key.init("b/*").get

    var
      putRes = await ds.put(key1, bytes1)

    assert putRes.isOk
    putRes = await ds.put(key2, bytes2)
    assert putRes.isOk
    putRes = await ds.put(key3, bytes3)
    assert putRes.isOk

    var
      kvs: seq[QueryResponse]

    var q = ds.query(); for kv in q(ds, Query.init(queryKey1)):
      let
        (key, data) = await kv

      kvs.add (key, data)

    check: kvs.sortedByIt(it.key.id) == @[
      (key: key1, data: bytes1),
      (key: key2, data: bytes2),
      (key: key3, data: bytes3)
    ].sortedByIt(it.key.id)

    kvs = @[]
    q = ds.query()

    let
      emptyKvs: seq[QueryResponse] = @[]

    for kv in q(ds, Query.init(queryKey2)):
      let
        (key, data) = await kv

      kvs.add (key, data)

    check: kvs == emptyKvs
