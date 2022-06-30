import std/options
import std/os

import pkg/asynctest/unittest2
import pkg/stew/results

import ../../datastore/async_sqlite_datastore

const
  bytes = @[1.byte, 2.byte, 3.byte]
  key = Key.init("a:b/c/d:e").get

var
  ds {.threadvar.}: AsyncSQLiteDatastore
  basePath {.threadvar.}: string
  basePathAbs {.threadvar.}: string
  filename {.threadvar.}: string
  dbPathAbs {.threadvar.}: string

suite "AsyncSQLiteDatastore":
  setup:
    # assumes tests/test_all is run from project root, e.g. with `nimble test`
    basePath = "tests" / "test_data"
    basePathAbs = getCurrentDir() / basePath
    filename = "test_store" & dbExt
    dbPathAbs = basePathAbs / filename

    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    discard dbPathAbs # suppresses "declared but not used" re: dbPathAbs

  teardown:
    if not ds.isNil: ds.close
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  test "new":
    let
      dsRes = AsyncSQLiteDatastore.new(basePathAbs, filename)

    check: dsRes.isOk

  test "accessors":
    ds = AsyncSQLiteDatastore()

    check: ds.store.isNil

    ds = AsyncSQLiteDatastore.new(basePath).get

    check: not ds.store.isNil

  test "helpers":
    ds = AsyncSQLiteDatastore.new(basePath).get

    check:
      parentDir(ds.store.dbPath) == basePathAbs
      not ds.store.env.isNil

    ds.close

    check:
      ds.store.isNil

  test "put":
    ds = AsyncSQLiteDatastore.new(basePathAbs, filename).get

    let
      putRes = await ds.put(key, bytes)

    check:
      putRes.isOk
      ds.store.get(key).get.get == bytes

  test "delete":
    ds = AsyncSQLiteDatastore.new(basePathAbs, filename).get

    let
      putRes = await ds.put(key, bytes)

    assert putRes.isOk
    assert ds.store.get(key).get.get == bytes

    let
      delRes = await ds.delete(key)

    check:
      delRes.isOk
      ds.store.get(key).get.isNone

  test "contains":
    ds = AsyncSQLiteDatastore.new(basePathAbs, filename).get

    let
      putRes = await ds.put(key, bytes)

    assert putRes.isOk

    var
      containsRes = await ds.contains(key)

    check:
      containsRes.isOk
      containsRes.get == true

    let
      key = Key.init("X/Y/Z").get

    containsRes = await ds.contains(key)

    check:
      containsRes.isOk
      containsRes.get == false

  test "get":
    ds = AsyncSQLiteDatastore.new(basePathAbs, filename).get

    assert not (await ds.contains(key)).get

    let
      putRes = await ds.put(key, bytes)

    assert putRes.isOk

    var
      getRes = await ds.get(key)
      getOpt = getRes.get

    check: getOpt.isSome and getOpt.get == bytes

    let
      key = Key.init("X/Y/Z").get

    assert not (await ds.contains(key)).get

    getRes = await ds.get(key)
    getOpt = getRes.get

    check: getOpt.isNone

  # test "query":
  #   check:
  #     true
