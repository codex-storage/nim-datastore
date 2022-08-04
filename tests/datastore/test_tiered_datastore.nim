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
    ds1: SQLiteDatastore
    ds2: FileSystemDatastore

  setup:
    removeDir(rootAbs)
    require(not dirExists(rootAbs))
    createDir(rootAbs)
    ds1 = SQLiteDatastore.new(memory).get
    ds2 = FileSystemDatastore.new(rootAbs).get

  teardown:
    if not ds1.isNil: ds1.close
    ds1 = nil
    removeDir(rootAbs)
    require(not dirExists(rootAbs))

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

    ds1.close
    ds1 = SQLiteDatastore.new(memory).get
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

  # asyncTest "query":
  #   check:
  #     true
