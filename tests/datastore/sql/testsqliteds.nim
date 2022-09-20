import std/options
import std/os

import pkg/asynctest/unittest2
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore/sql/sqliteds

import ../basictests

suite "Test Basic SQLiteDatastore":
  let
    (path, _, _) = instantiationInfo(-1, fullPaths = true) # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    filename = "test_store" & DbExt
    dbPathAbs = basePathAbs / filename
    key = Key.init("a:b/c/d:e").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  var
    dsDb: SQLiteDatastore

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    dsDb = SQLiteDatastore.new(path = dbPathAbs).tryGet()

  teardownAll:
    (await dsDb.close()).tryGet()

    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  basicStoreTests(dsDb, key, bytes, otherBytes)

suite "Test Read Only SQLiteDatastore":
  let
    (path, _, _) = instantiationInfo(-1, fullPaths = true) # get this file's name
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
      not (await readOnlyDb.contains(key)).tryGet()
      not (await dsDb.contains(key)).tryGet()

  # test "query":
  #   ds = SQLiteDatastore.new(basePathAbs, filename).get

  #   var
  #     key1 = Key.init("a").get
  #     key2 = Key.init("a/b").get
  #     key3 = Key.init("a/b:c").get
  #     key4 = Key.init("a:b").get
  #     key5 = Key.init("a:b/c").get
  #     key6 = Key.init("a:b/c:d").get
  #     key7 = Key.init("A").get
  #     key8 = Key.init("A/B").get
  #     key9 = Key.init("A/B:C").get
  #     key10 = Key.init("A:B").get
  #     key11 = Key.init("A:B/C").get
  #     key12 = Key.init("A:B/C:D").get

  #     bytes1  = @[1.byte, 2.byte, 3.byte]
  #     bytes2  = @[4.byte, 5.byte, 6.byte]
  #     bytes3: seq[byte] = @[]
  #     bytes4  = bytes1
  #     bytes5  = bytes2
  #     bytes6  = bytes3
  #     bytes7  = bytes1
  #     bytes8  = bytes2
  #     bytes9  = bytes3
  #     bytes10  = bytes1
  #     bytes11  = bytes2
  #     bytes12  = bytes3

  #     queryKey = Key.init("*").get

  #   var
  #     putRes = await ds.put(key1, bytes1)

  #   assert putRes.isOk
  #   putRes = await ds.put(key2, bytes2)
  #   assert putRes.isOk
  #   putRes = await ds.put(key3, bytes3)
  #   assert putRes.isOk
  #   putRes = await ds.put(key4, bytes4)
  #   assert putRes.isOk
  #   putRes = await ds.put(key5, bytes5)
  #   assert putRes.isOk
  #   putRes = await ds.put(key6, bytes6)
  #   assert putRes.isOk
  #   putRes = await ds.put(key7, bytes7)
  #   assert putRes.isOk
  #   putRes = await ds.put(key8, bytes8)
  #   assert putRes.isOk
  #   putRes = await ds.put(key9, bytes9)
  #   assert putRes.isOk
  #   putRes = await ds.put(key10, bytes10)
  #   assert putRes.isOk
  #   putRes = await ds.put(key11, bytes11)
  #   assert putRes.isOk
  #   putRes = await ds.put(key12, bytes12)
  #   assert putRes.isOk

  #   var
  #     kds: seq[QueryResponse]

  #   for kd in ds.query(Query.init(queryKey)):
  #     let
  #       (key, data) = await kd

  #     kds.add (key, data)

  #   # see https://sqlite.org/lang_select.html#the_order_by_clause
  #   # If a SELECT statement that returns more than one row does not have an
  #   # ORDER BY clause, the order in which the rows are returned is undefined.

  #   check: kds.sortedByIt(it.key.id) == @[
  #     (key: key1, data: bytes1),
  #     (key: key2, data: bytes2),
  #     (key: key3, data: bytes3),
  #     (key: key4, data: bytes4),
  #     (key: key5, data: bytes5),
  #     (key: key6, data: bytes6),
  #     (key: key7, data: bytes7),
  #     (key: key8, data: bytes8),
  #     (key: key9, data: bytes9),
  #     (key: key10, data: bytes10),
  #     (key: key11, data: bytes11),
  #     (key: key12, data: bytes12)
  #   ].sortedByIt(it.key.id)

  #   kds = @[]

  #   queryKey = Key.init("a*").get

  #   for kd in ds.query(Query.init(queryKey)):
  #     let
  #       (key, data) = await kd

  #     kds.add (key, data)

  #   check: kds.sortedByIt(it.key.id) == @[
  #     (key: key1, data: bytes1),
  #     (key: key2, data: bytes2),
  #     (key: key3, data: bytes3),
  #     (key: key4, data: bytes4),
  #     (key: key5, data: bytes5),
  #     (key: key6, data: bytes6)
  #   ].sortedByIt(it.key.id)

  #   kds = @[]

  #   queryKey = Key.init("A*").get

  #   for kd in ds.query(Query.init(queryKey)):
  #     let
  #       (key, data) = await kd

  #     kds.add (key, data)

  #   check: kds.sortedByIt(it.key.id) == @[
  #     (key: key7, data: bytes7),
  #     (key: key8, data: bytes8),
  #     (key: key9, data: bytes9),
  #     (key: key10, data: bytes10),
  #     (key: key11, data: bytes11),
  #     (key: key12, data: bytes12)
  #   ].sortedByIt(it.key.id)

  #   kds = @[]

  #   queryKey = Key.init("a/?").get

  #   for kd in ds.query(Query.init(queryKey)):
  #     let
  #       (key, data) = await kd

  #     kds.add (key, data)

  #   check: kds.sortedByIt(it.key.id) == @[
  #     (key: key2, data: bytes2)
  #   ].sortedByIt(it.key.id)

  #   kds = @[]

  #   queryKey = Key.init("A/?").get

  #   for kd in ds.query(Query.init(queryKey)):
  #     let
  #       (key, data) = await kd

  #     kds.add (key, data)

  #   check: kds.sortedByIt(it.key.id) == @[
  #     (key: key8, data: bytes8)
  #   ].sortedByIt(it.key.id)

  #   kds = @[]

  #   queryKey = Key.init("*/?").get

  #   for kd in ds.query(Query.init(queryKey)):
  #     let
  #       (key, data) = await kd

  #     kds.add (key, data)

  #   check: kds.sortedByIt(it.key.id) == @[
  #     (key: key2, data: bytes2),
  #     (key: key5, data: bytes5),
  #     (key: key8, data: bytes8),
  #     (key: key11, data: bytes11)
  #   ].sortedByIt(it.key.id)

  #   kds = @[]

  #   queryKey = Key.init("[Aa]/?").get

  #   for kd in ds.query(Query.init(queryKey)):
  #     let
  #       (key, data) = await kd

  #     kds.add (key, data)

  #   check: kds.sortedByIt(it.key.id) == @[
  #     (key: key2, data: bytes2),
  #     (key: key8, data: bytes8)
  #   ].sortedByIt(it.key.id)

  #   kds = @[]

  #   # SQLite's GLOB operator, akin to Unix file globbing syntax, is greedy re:
  #   # wildcard "*". So a pattern such as "a:*[^/]" will not restrict results to
  #   # "/a:b", i.e. it will match on "/a:b", "/a:b/c" and "/a:b/c:d".

  #   queryKey = Key.init("a:*[^/]").get

  #   for kd in ds.query(Query.init(queryKey)):
  #     let
  #       (key, data) = await kd

  #     kds.add (key, data)

  #   check: kds.sortedByIt(it.key.id) == @[
  #     (key: key4, data: bytes4),
  #     (key: key5, data: bytes5),
  #     (key: key6, data: bytes6)
  #   ].sortedByIt(it.key.id)

  #   kds = @[]

  #   queryKey = Key.init("a:*[Bb]").get

  #   for kd in ds.query(Query.init(queryKey)):
  #     let
  #       (key, data) = await kd

  #     kds.add (key, data)

  #   check: kds.sortedByIt(it.key.id) == @[
  #     (key: key4, data: bytes4)
  #   ].sortedByIt(it.key.id)

  #   kds = @[]

  #   var
  #     deleteRes = await ds.delete(key1)

  #   assert deleteRes.isOk
  #   deleteRes = await ds.delete(key2)
  #   assert deleteRes.isOk
  #   deleteRes = await ds.delete(key3)
  #   assert deleteRes.isOk
  #   deleteRes = await ds.delete(key4)
  #   assert deleteRes.isOk
  #   deleteRes = await ds.delete(key5)
  #   assert deleteRes.isOk
  #   deleteRes = await ds.delete(key6)
  #   assert deleteRes.isOk
  #   deleteRes = await ds.delete(key7)
  #   assert deleteRes.isOk
  #   deleteRes = await ds.delete(key8)
  #   assert deleteRes.isOk
  #   deleteRes = await ds.delete(key9)
  #   assert deleteRes.isOk
  #   deleteRes = await ds.delete(key10)
  #   assert deleteRes.isOk
  #   deleteRes = await ds.delete(key11)
  #   assert deleteRes.isOk
  #   deleteRes = await ds.delete(key12)
  #   assert deleteRes.isOk

  #   let
  #     emptyKds: seq[QueryResponse] = @[]

  #   for kd in ds.query(Query.init(queryKey)):
  #     let
  #       (key, data) = await kd

  #     kds.add (key, data)

  #   check: kds == emptyKds
