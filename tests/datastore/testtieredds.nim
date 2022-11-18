import std/options
import std/os

import pkg/asynctest
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore/fsds
import pkg/datastore/sql
import pkg/datastore/tieredds

import ./dscommontests

suite "Test Basic Tired Datastore":
  let
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes
    key = Key.init("a:b/c/d:e").get
    root = "tests" / "test_data"
    path = currentSourcePath() # get this file's name
    rootAbs = path.parentDir / root

  var
    ds1: SQLiteDatastore
    ds2: FSDatastore
    tiredDs: TieredDatastore

  setupAll:
    removeDir(rootAbs)
    require(not dirExists(rootAbs))
    createDir(rootAbs)

    ds1 = SQLiteDatastore.new(Memory).tryGet
    ds2 = FSDatastore.new(rootAbs, depth = 5).tryGet
    tiredDs = TieredDatastore.new(@[ds1, ds2]).tryGet

  teardownAll:
    removeDir(rootAbs)
    require(not dirExists(rootAbs))

  basicStoreTests(tiredDs, key, bytes, otherBytes)

suite "TieredDatastore":
  # assumes tests/test_all is run from project root, e.g. with `nimble test`
  let
    bytes = @[1.byte, 2.byte, 3.byte]
    key = Key.init("a:b/c/d:e").get
    root = "tests" / "test_data"
    path = currentSourcePath() # get this file's name
    rootAbs = path.parentDir / root

  var
    ds1: SQLiteDatastore
    ds2: FSDatastore

  setup:
    removeDir(rootAbs)
    require(not dirExists(rootAbs))
    createDir(rootAbs)
    ds1 = SQLiteDatastore.new(Memory).get
    ds2 = FSDatastore.new(rootAbs, depth = 5).get

  teardown:
    if not ds1.isNil:
      discard await ds1.close

    ds1 = nil

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
      TieredDatastore.new(ds1, ds2).tryGet.stores == stores
      TieredDatastore.new([ds1, ds2]).tryGet.stores == stores
      TieredDatastore.new(@[ds1, ds2]).tryGet.stores == stores

  test "put":
    let
      ds = TieredDatastore.new(ds1, ds2).get
      putRes = await ds.put(key, bytes)

    check:
      putRes.isOk
      (await ds1.get(key)).tryGet == bytes
      (await ds2.get(key)).tryGet == bytes

  test "delete":
    let
      ds = TieredDatastore.new(ds1, ds2).get

    (await ds.put(key, bytes)).tryGet
    (await ds.delete(key)).tryGet

    check:
      (await ds1.get(key)).tryGet.len == 0

    expect DatastoreKeyNotFound:
      discard (await ds2.get(key)).tryGet

  test "contains":
    let
      ds = TieredDatastore.new(ds1, ds2).tryGet

    check:
      not (await ds1.contains(key)).tryGet
      not (await ds2.contains(key)).tryGet

    (await ds.put(key, bytes)).tryGet

    check:
      (await ds.contains(key)).tryGet
      (await ds1.contains(key)).tryGet
      (await ds2.contains(key)).tryGet

  test "get":
    var
      ds = TieredDatastore.new(ds1, ds2).tryGet

    check:
      not (await ds1.contains(key)).tryGet
      not (await ds2.contains(key)).tryGet
      not (await ds.contains(key)).tryGet

    (await ds.put(key, bytes)).tryGet

    check:
      (await ds.get(key)).tryGet == bytes
      (await ds1.get(key)).tryGet == bytes
      (await ds2.get(key)).tryGet == bytes

    (await ds1.close()).tryGet
    ds1 = nil

    ds1 = SQLiteDatastore.new(Memory).tryGet
    ds = TieredDatastore.new(ds1, ds2).tryGet

    check:
      not (await ds1.contains(key)).tryGet
      (await ds2.get(key)).tryGet == bytes
      (await ds.get(key)).tryGet == bytes
      (await ds1.get(key)).tryGet == bytes
