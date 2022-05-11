import std/options
import std/os

import pkg/stew/results
import pkg/unittest2

import ../../datastore/filesystem_datastore
import ../../datastore/sqlite_datastore
import ../../datastore/tiered_datastore

suite "TieredDatastore":
  setup:
    # assumes tests/test_all is run from project root, e.g. with `nimble test`
    let
      bytes = @[1.byte, 2.byte, 3.byte]
      key = Key.init("a:b/c/d:e").get
      root = "tests" / "test_data"
      rootAbs = getCurrentDir() / root

    discard bytes # suppresses "declared but not used" re: bytes
    discard key # # suppresses "declared but not used" re: key

    removeDir(rootAbs)
    require(not dirExists(rootAbs))

    var
      ds1 = SQLiteDatastore.new(inMemory = true).get
      ds2 = FileSystemDatastore.new(rootAbs).get

    discard ds2 # suppresses "declared but not used" re: ds2

  teardown:
    ds1.close
    removeDir(rootAbs)
    require(not dirExists(rootAbs))

  test "new":
    check:
      TieredDatastore.new().isErr
      TieredDatastore.new([]).isErr
      TieredDatastore.new(@[]).isErr
      TieredDatastore.new(ds1, ds2).isOk
      TieredDatastore.new([ds1, ds2]).isOk
      TieredDatastore.new(@[ds1, ds2]).isOk

  test "accessors":
    let
      stores = @[ds1, ds2]

    check:
      TieredDatastore.new(ds1, ds2).get.stores == stores
      TieredDatastore.new([ds1, ds2]).get.stores == stores
      TieredDatastore.new(@[ds1, ds2]).get.stores == stores

  test "put":
    let
      ds = TieredDatastore.new(ds1, ds2).get

    assert ds1.get(key).get.isNone
    assert ds2.get(key).get.isNone

    let
      putRes = ds.put(key, bytes)

    check:
      putRes.isOk
      ds1.get(key).get.get == bytes
      ds2.get(key).get.get == bytes

  test "delete":
    let
      ds = TieredDatastore.new(ds1, ds2).get
      putRes = ds.put(key, bytes)

    assert putRes.isOk
    assert ds1.get(key).get.get == bytes
    assert ds2.get(key).get.get == bytes

    let
      delRes = ds.delete(key)

    check:
      delRes.isOk
      ds1.get(key).get.isNone
      ds2.get(key).get.isNone

  test "contains":
    let
      ds = TieredDatastore.new(ds1, ds2).get

    assert not ds1.contains(key).get
    assert not ds2.contains(key).get

    let
      putRes = ds.put(key, bytes)

    assert putRes.isOk

    let
      containsRes = ds.contains(key)

    check:
      containsRes.isOk
      containsRes.get
      ds1.contains(key).get
      ds2.contains(key).get

  test "get":
    var
      ds = TieredDatastore.new(ds1, ds2).get

    assert ds1.get(key).get.isNone
    assert ds2.get(key).get.isNone

    check: ds.get(key).get.isNone

    let
      putRes = ds.put(key, bytes)

    assert putRes.isOk

    var
      getRes = ds.get(key)

    check:
      getRes.isOk
      getRes.get.isSome
      getRes.get.get == bytes
      ds1.get(key).get.isSome
      ds2.get(key).get.isSome
      ds1.get(key).get.get == bytes
      ds2.get(key).get.get == bytes

    ds1.close
    ds1 = SQLiteDatastore.new(inMemory = true).get
    ds = TieredDatastore.new(ds1, ds2).get

    assert ds1.get(key).get.isNone
    assert ds2.get(key).get.isSome
    assert ds2.get(key).get.get == bytes

    getRes = ds.get(key)

    check:
      getRes.isOk
      getRes.get.isSome
      getRes.get.get == bytes
      ds1.get(key).get.isSome
      ds1.get(key).get.get == bytes

  # test "query":
  #   check:
  #     true
