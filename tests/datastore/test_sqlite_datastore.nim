import std/algorithm
import std/options
import std/os

import pkg/asynctest/unittest2
import pkg/chronos
import pkg/stew/results

import ../../datastore/sqlite_datastore
import ./templates

suite "SQLiteDatastore":
  var
    ds: SQLiteDatastore

  # assumes tests/test_all is run from project root, e.g. with `nimble test`
  let
    basePath = "tests" / "test_data"
    basePathAbs = getCurrentDir() / basePath
    filename = "test_store" & dbExt
    dbPathAbs = basePathAbs / filename

  setup:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

  teardown:
    if not ds.isNil: ds.close
    ds = nil
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  asyncTest "new":
    var
      dsRes = SQLiteDatastore.new(basePathAbs, filename, readOnly = true)

    # for `readOnly = true` to succeed the database file must already exist
    check: dsRes.isErr

    dsRes = SQLiteDatastore.new(basePathAbs / "missing", filename)

    check: dsRes.isErr

    dsRes = SQLiteDatastore.new(basePathAbs, filename, pageSize = 65536,
      cacheSize = 1600, journalMode = TRUNCATE)

    check:
      dsRes.isOk
      fileExists(dbPathAbs)

    dsRes.get.close
    removeDir(basePathAbs)
    assert not dirExists(basePathAbs)
    createDir(basePathAbs)

    dsRes = SQLiteDatastore.new(basePath, filename)

    check:
      dsRes.isOk
      fileExists(dbPathAbs)

    dsRes.get.close

    # for `readOnly = true` to succeed the database file must already exist, so
    # the existing file (per previous step) is not deleted prior to the next
    # invocation of `SQLiteDatastore.new`

    dsRes = SQLiteDatastore.new(basePath, filename, readOnly = true)

    check: dsRes.isOk

    dsRes.get.close
    removeDir(basePathAbs)
    assert not dirExists(basePathAbs)
    createDir(basePathAbs)

    dsRes = SQLiteDatastore.new(memory)

    check: dsRes.isOk

    dsRes.get.close

    dsRes = SQLiteDatastore.new(memory, readOnly = true)

    check: dsRes.isErr

  asyncTest "accessors":
    ds = SQLiteDatastore.new(basePath).get

    check:
      parentDir(ds.dbPath) == basePathAbs
      not ds.env.isNil

  asyncTest "helpers":
    ds = SQLiteDatastore.new(basePath).get

    ds.close

    check:
      ds.env.isNil
      timestamp(10.123_456) == 10_123_456.int64

  asyncTest "put":
    let
      key = Key.init("a:b/c/d:e").get

    # for `readOnly = true` to succeed the database file must already exist
    ds = SQLiteDatastore.new(basePathAbs, filename).get
    ds.close
    ds = SQLiteDatastore.new(basePathAbs, filename, readOnly = true).get

    var
      bytes: seq[byte]
      timestamp = timestamp()
      putRes = await ds.put(key, bytes, timestamp)

    check: putRes.isErr

    ds.close
    removeDir(basePathAbs)
    assert not dirExists(basePathAbs)
    createDir(basePathAbs)

    ds = SQLiteDatastore.new(basePathAbs, filename).get

    timestamp = timestamp()
    putRes = await ds.put(key, bytes, timestamp)

    check: putRes.isOk

    let
      prequeryRes = NoParamsStmt.prepare(
        ds.env, "SELECT timestamp AS foo, id AS baz, data AS bar FROM " &
          tableName & ";")

    assert prequeryRes.isOk

    let
      prequery = prequeryRes.get
      idCol = idCol(RawStmtPtr(prequery), 1)
      dataCol = dataCol(RawStmtPtr(prequery), 2)
      timestampCol = timestampCol(RawStmtPtr(prequery), 0)

    var
      qId: string
      qData: seq[byte]
      qTimestamp: int64
      rowCount = 0

    proc onData(s: RawStmtPtr) {.closure.} =
      qId = idCol()
      qData = dataCol()
      qTimestamp = timestampCol()
      inc rowCount

    var
      qRes = prequery.query((), onData)

    assert qRes.isOk

    check:
      qRes.get
      qId == key.id
      qData == bytes
      qTimestamp == timestamp
      rowCount == 1

    bytes = @[1.byte, 2.byte, 3.byte]
    timestamp = timestamp()
    putRes = await ds.put(key, bytes, timestamp)

    check: putRes.isOk

    rowCount = 0
    qRes = prequery.query((), onData)
    assert qRes.isOk

    check:
      qRes.get
      qId == key.id
      qData == bytes
      qTimestamp == timestamp
      rowCount == 1

    bytes = @[4.byte, 5.byte, 6.byte]
    timestamp = timestamp()
    putRes = await ds.put(key, bytes, timestamp)

    check: putRes.isOk

    rowCount = 0
    qRes = prequery.query((), onData)
    assert qRes.isOk

    check:
      qRes.get
      qId == key.id
      qData == bytes
      qTimestamp == timestamp
      rowCount == 1

    prequery.dispose

  asyncTest "delete":
    let
      bytes = @[1.byte, 2.byte, 3.byte]

    var
      key = Key.init("a:b/c/d:e").get

    # for `readOnly = true` to succeed the database file must already exist
    ds = SQLiteDatastore.new(basePathAbs, filename).get
    ds.close
    ds = SQLiteDatastore.new(basePathAbs, filename, readOnly = true).get

    var
      delRes = await ds.delete(key)

    check: delRes.isErr

    ds.close
    removeDir(basePathAbs)
    assert not dirExists(basePathAbs)
    createDir(basePathAbs)

    ds = SQLiteDatastore.new(basePathAbs, filename).get

    let
      putRes = await ds.put(key, bytes)

    assert putRes.isOk

    let
      query = "SELECT * FROM " & tableName & ";"

    var
      rowCount = 0

    proc onData(s: RawStmtPtr) {.closure.} =
      inc rowCount

    var
      qRes = ds.env.query(query, onData)

    assert qRes.isOk
    check: rowCount == 1
    delRes = await ds.delete(key)

    check: delRes.isOk

    rowCount = 0
    qRes = ds.env.query(query, onData)
    assert qRes.isOk

    check:
      delRes.isOk
      rowCount == 0

    key = Key.init("X/Y/Z").get

    delRes = await ds.delete(key)

    check: delRes.isOk

  asyncTest "contains":
    let
      bytes = @[1.byte, 2.byte, 3.byte]

    var
      key = Key.init("a:b/c/d:e").get

    ds = SQLiteDatastore.new(basePathAbs, filename).get

    let
      putRes = await ds.put(key, bytes)

    assert putRes.isOk

    var
      containsRes = await ds.contains(key)

    check:
      containsRes.isOk
      containsRes.get == true

    key = Key.init("X/Y/Z").get

    containsRes = await ds.contains(key)

    check:
      containsRes.isOk
      containsRes.get == false

  asyncTest "get":
    ds = SQLiteDatastore.new(basePathAbs, filename).get

    var
      bytes: seq[byte]
      key = Key.init("a:b/c/d:e").get
      putRes = await ds.put(key, bytes)

    assert putRes.isOk

    var
      getRes = await ds.get(key)
      getOpt = getRes.get

    check: getOpt.isSome and getOpt.get == bytes

    bytes = @[1.byte, 2.byte, 3.byte]
    putRes = await ds.put(key, bytes)

    assert putRes.isOk

    getRes = await ds.get(key)
    getOpt = getRes.get

    check: getOpt.isSome and getOpt.get == bytes

    key = Key.init("X/Y/Z").get

    assert not (await ds.contains(key)).get

    getRes = await ds.get(key)
    getOpt = getRes.get

    check: getOpt.isNone

  asyncTest "query":
    ds = SQLiteDatastore.new(basePathAbs, filename).get

    var
      key1 = Key.init("a").get
      key2 = Key.init("a/b").get
      key3 = Key.init("a/b:c").get
      key4 = Key.init("a:b").get
      key5 = Key.init("a:b/c").get
      key6 = Key.init("a:b/c:d").get
      key7 = Key.init("A").get
      key8 = Key.init("A/B").get
      key9 = Key.init("A/B:C").get
      key10 = Key.init("A:B").get
      key11 = Key.init("A:B/C").get
      key12 = Key.init("A:B/C:D").get

      bytes1  = @[1.byte, 2.byte, 3.byte]
      bytes2  = @[4.byte, 5.byte, 6.byte]
      bytes3: seq[byte] = @[]
      bytes4  = bytes1
      bytes5  = bytes2
      bytes6  = bytes3
      bytes7  = bytes1
      bytes8  = bytes2
      bytes9  = bytes3
      bytes10  = bytes1
      bytes11  = bytes2
      bytes12  = bytes3

      queryKey = Key.init("*").get

    var
      putRes = await ds.put(key1, bytes1)

    assert putRes.isOk
    putRes = await ds.put(key2, bytes2)
    assert putRes.isOk
    putRes = await ds.put(key3, bytes3)
    assert putRes.isOk
    putRes = await ds.put(key4, bytes4)
    assert putRes.isOk
    putRes = await ds.put(key5, bytes5)
    assert putRes.isOk
    putRes = await ds.put(key6, bytes6)
    assert putRes.isOk
    putRes = await ds.put(key7, bytes7)
    assert putRes.isOk
    putRes = await ds.put(key8, bytes8)
    assert putRes.isOk
    putRes = await ds.put(key9, bytes9)
    assert putRes.isOk
    putRes = await ds.put(key10, bytes10)
    assert putRes.isOk
    putRes = await ds.put(key11, bytes11)
    assert putRes.isOk
    putRes = await ds.put(key12, bytes12)
    assert putRes.isOk

    var
      kds: seq[QueryResponse]

    for kd in ds.query(Query.init(queryKey)):
      let
        (key, data) = await kd

      kds.add (key, data)

    # see https://sqlite.org/lang_select.html#the_order_by_clause
    # If a SELECT statement that returns more than one row does not have an
    # ORDER BY clause, the order in which the rows are returned is undefined.

    check: kds.sortedByIt(it.key.id) == @[
      (key: key1, data: bytes1),
      (key: key2, data: bytes2),
      (key: key3, data: bytes3),
      (key: key4, data: bytes4),
      (key: key5, data: bytes5),
      (key: key6, data: bytes6),
      (key: key7, data: bytes7),
      (key: key8, data: bytes8),
      (key: key9, data: bytes9),
      (key: key10, data: bytes10),
      (key: key11, data: bytes11),
      (key: key12, data: bytes12)
    ].sortedByIt(it.key.id)

    kds = @[]

    queryKey = Key.init("a*").get

    for kd in ds.query(Query.init(queryKey)):
      let
        (key, data) = await kd

      kds.add (key, data)

    check: kds.sortedByIt(it.key.id) == @[
      (key: key1, data: bytes1),
      (key: key2, data: bytes2),
      (key: key3, data: bytes3),
      (key: key4, data: bytes4),
      (key: key5, data: bytes5),
      (key: key6, data: bytes6)
    ].sortedByIt(it.key.id)

    kds = @[]

    queryKey = Key.init("A*").get

    for kd in ds.query(Query.init(queryKey)):
      let
        (key, data) = await kd

      kds.add (key, data)

    check: kds.sortedByIt(it.key.id) == @[
      (key: key7, data: bytes7),
      (key: key8, data: bytes8),
      (key: key9, data: bytes9),
      (key: key10, data: bytes10),
      (key: key11, data: bytes11),
      (key: key12, data: bytes12)
    ].sortedByIt(it.key.id)

    kds = @[]

    queryKey = Key.init("a/?").get

    for kd in ds.query(Query.init(queryKey)):
      let
        (key, data) = await kd

      kds.add (key, data)

    check: kds.sortedByIt(it.key.id) == @[
      (key: key2, data: bytes2)
    ].sortedByIt(it.key.id)

    kds = @[]

    queryKey = Key.init("A/?").get

    for kd in ds.query(Query.init(queryKey)):
      let
        (key, data) = await kd

      kds.add (key, data)

    check: kds.sortedByIt(it.key.id) == @[
      (key: key8, data: bytes8)
    ].sortedByIt(it.key.id)

    kds = @[]

    queryKey = Key.init("*/?").get

    for kd in ds.query(Query.init(queryKey)):
      let
        (key, data) = await kd

      kds.add (key, data)

    check: kds.sortedByIt(it.key.id) == @[
      (key: key2, data: bytes2),
      (key: key5, data: bytes5),
      (key: key8, data: bytes8),
      (key: key11, data: bytes11)
    ].sortedByIt(it.key.id)

    kds = @[]

    queryKey = Key.init("[Aa]/?").get

    for kd in ds.query(Query.init(queryKey)):
      let
        (key, data) = await kd

      kds.add (key, data)

    check: kds.sortedByIt(it.key.id) == @[
      (key: key2, data: bytes2),
      (key: key8, data: bytes8)
    ].sortedByIt(it.key.id)

    kds = @[]

    # SQLite's GLOB operator, akin to Unix file globbing syntax, is greedy re:
    # wildcard "*". So a pattern such as "a:*[^/]" will not restrict results to
    # "/a:b", i.e. it will match on "/a:b", "/a:b/c" and "/a:b/c:d".

    queryKey = Key.init("a:*[^/]").get

    for kd in ds.query(Query.init(queryKey)):
      let
        (key, data) = await kd

      kds.add (key, data)

    check: kds.sortedByIt(it.key.id) == @[
      (key: key4, data: bytes4),
      (key: key5, data: bytes5),
      (key: key6, data: bytes6)
    ].sortedByIt(it.key.id)

    kds = @[]

    queryKey = Key.init("a:*[Bb]").get

    for kd in ds.query(Query.init(queryKey)):
      let
        (key, data) = await kd

      kds.add (key, data)

    check: kds.sortedByIt(it.key.id) == @[
      (key: key4, data: bytes4)
    ].sortedByIt(it.key.id)

    kds = @[]

    var
      deleteRes = await ds.delete(key1)

    assert deleteRes.isOk
    deleteRes = await ds.delete(key2)
    assert deleteRes.isOk
    deleteRes = await ds.delete(key3)
    assert deleteRes.isOk
    deleteRes = await ds.delete(key4)
    assert deleteRes.isOk
    deleteRes = await ds.delete(key5)
    assert deleteRes.isOk
    deleteRes = await ds.delete(key6)
    assert deleteRes.isOk
    deleteRes = await ds.delete(key7)
    assert deleteRes.isOk
    deleteRes = await ds.delete(key8)
    assert deleteRes.isOk
    deleteRes = await ds.delete(key9)
    assert deleteRes.isOk
    deleteRes = await ds.delete(key10)
    assert deleteRes.isOk
    deleteRes = await ds.delete(key11)
    assert deleteRes.isOk
    deleteRes = await ds.delete(key12)
    assert deleteRes.isOk

    let
      emptyKds: seq[QueryResponse] = @[]

    for kd in ds.query(Query.init(queryKey)):
      let
        (key, data) = await kd

      kds.add (key, data)

    check: kds == emptyKds
