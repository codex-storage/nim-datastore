import std/options
import std/os
import std/tables

import pkg/asynctest
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore/mountedds
import pkg/datastore/sql
import pkg/datastore/fsds

import ./dscommontests

suite "Test Basic Mounted Datastore":
  let
    root = "tests" / "test_data"
    (path, _, _) = instantiationInfo(-1, fullPaths = true) # get this file's name
    rootAbs = path.parentDir / root

    key = Key.init("a:b/c/d:e").get
    sqlKey = Key.init("/sql").tryGet
    fsKey = Key.init("/fs").tryGet

    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  var
    sql: SQLiteDatastore
    fs: FSDatastore
    mountedDs: MountedDatastore

  setupAll:
    removeDir(rootAbs)
    require(not dirExists(rootAbs))
    createDir(rootAbs)

    sql = SQLiteDatastore.new(Memory).tryGet
    fs = FSDatastore.new(rootAbs, depth = 5).tryGet
    mountedDs = MountedDatastore.new({
      sqlKey: Datastore(sql),
      fsKey: Datastore(fs)}.toTable)
      .tryGet

  teardownAll:
    removeDir(rootAbs)
    require(not dirExists(rootAbs))

  suite "Mounted sql":
    basicStoreTests(mountedDs, Key.init(sqlKey, key).tryGet, bytes, otherBytes)

  suite "Mounted fs":
    basicStoreTests(mountedDs, Key.init(fsKey, key).tryGet, bytes, otherBytes)

suite "Test Mounted Datastore":

  test "Should mount datastore":
    let
      ds = SQLiteDatastore.new(Memory).tryGet
      mounted = MountedDatastore.new().tryGet
      key = Key.init("/sql").tryGet

    mounted.mount(key, ds).tryGet

    check: mounted.stores.len == 1
    mounted.stores.withValue(key, store):
      check:
        store.key == key
        store.store == ds

  test "Should find with exact key":
    let
      ds = SQLiteDatastore.new(Memory).tryGet
      key = Key.init("/sql").tryGet
      mounted = MountedDatastore.new({key: Datastore(ds)}.toTable).tryGet
      store = mounted.findStore(key).tryGet

    check:
      store.key == key
      store.store == ds

  test "Should find with child key":
    let
      ds = SQLiteDatastore.new(Memory).tryGet
      key = Key.init("/sql").tryGet
      childKey = Key.init("/sql/child/key").tryGet
      mounted = MountedDatastore.new({key: Datastore(ds)}.toTable).tryGet
      store = mounted.findStore(childKey).tryGet

    check:
      store.key == key
      store.store == ds

  test "Should error on missing key":
    let
      ds = SQLiteDatastore.new(Memory).tryGet
      key = Key.init("/sql").tryGet
      childKey = Key.init("/nomatchkey/child/key").tryGet
      mounted = MountedDatastore.new({key: Datastore(ds)}.toTable).tryGet

    expect DatastoreKeyNotFound:
      discard mounted.findStore(childKey).tryGet

  test "Should find nested stores":
    let
      ds1 = SQLiteDatastore.new(Memory).tryGet
      ds2 = SQLiteDatastore.new(Memory).tryGet
      key1 = Key.init("/sql").tryGet
      key2 = Key.init("/sql/nested").tryGet

      nestedKey1 = Key.init("/sql/anotherkey").tryGet
      nestedKey2 = Key.init("/sql/nested/key").tryGet

      mounted = MountedDatastore.new({
        key1: Datastore(ds1),
        key2: Datastore(ds2)}.toTable).tryGet

      store1 = mounted.findStore(nestedKey1).tryGet
      store2 = mounted.findStore(nestedKey2).tryGet

    check:
      store1.key == key1
      store1.store == ds1

      store2.key == key2
      store2.store == ds2
