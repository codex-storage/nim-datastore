import std/options
import std/os
import std/sequtils
from std/algorithm import sort, reversed

import pkg/unittest2
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore/sql/sqliteds
import pkg/datastore/key

import ../dscommontests
import ../querycommontests

proc testBasic[K, V, B](
  ds: SQLiteBackend,
  key: K,
  bytes: V,
  otherBytes: V,
  batch: B,
) =

  test "put":
    ds.put(key, bytes).tryGet()

  test "get":
    check:
      ds.get(key).tryGet() == bytes

  test "put update":
    ds.put(key, otherBytes).tryGet()

  test "get updated":
    check:
      ds.get(key).tryGet() == otherBytes

  test "delete":
    ds.delete(key).tryGet()

  test "contains":
    check key notin ds

  test "put batch":

    ds.put(batch).tryGet

    for (k, v) in batch:
      check: ds.has(k).tryGet

  test "delete batch":
    var keys: seq[K]
    for (k, v) in batch:
      keys.add(k)

    ds.delete(keys).tryGet

    for (k, v) in batch:
      check: not ds.has(k).tryGet

  test "handle missing key":
    let key = Key.init("/missing/key").tryGet().id()

    expect(DatastoreKeyNotFound):
      discard ds.get(key).tryGet() # non existing key

suite "Test Basic SQLiteDatastore":
  let
    ds = SQLiteBackend.new(Memory).tryGet()
    keyFull = Key.init("a:b/c/d:e").tryGet()
    key = keyFull.id()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  var batch: seq[tuple[key: string, data: seq[byte]]]
  for k in 0..<100:
    let kk = Key.init(key, $k).tryGet().id()
    batch.add( (kk, @[k.byte]) )

  suiteTeardown:
    ds.close().tryGet()

  testBasic(ds, key, bytes, otherBytes, batch)

suite "Test DataBuffer SQLiteDatastore":
  let
    ds = SQLiteBackend.new(Memory).tryGet()
    keyFull = Key.init("a:b/c/d:e").tryGet()
    key = KeyId.new keyFull.id()
    bytes = DataBuffer.new "some bytes"
    otherBytes = DataBuffer.new "some other bytes"

  var batch: seq[tuple[key: KeyId, data: DataBuffer]]
  for k in 0..<100:
    let kk = Key.init(keyFull.id(), $k).tryGet().id()
    batch.add( (KeyId.new kk, DataBuffer.new @[k.byte]) )

  suiteTeardown:
    ds.close().tryGet()

  testBasic(ds, key, bytes, otherBytes, batch)

suite "queryTests":
  let
    ds = SQLiteBackend.new(Memory).tryGet()

  var
    key1: KeyId
    key2: KeyId
    key3: KeyId
    val1: DataBuffer
    val2: DataBuffer
    val3: DataBuffer

  setup:
    key1 = KeyId.new "/a"
    key2 = KeyId.new "/a/b"
    key3 = KeyId.new "/a/b/c"
    val1 = DataBuffer.new "value for 1"
    val2 = DataBuffer.new "value for 2"
    val3 = DataBuffer.new "value for 3"

  test "Key should query all keys and all it's children":
    let
      q = DbQuery(key: key1)

    ds.put(key1, val1).tryGet
    ds.put(key2, val2).tryGet
    ds.put(key3, val3).tryGet

    let
      (handle, iter) = ds.query(q).tryGet
      res = iter.mapIt(it.tryGet())

    check:
      res.len == 3
      res[0].key.get == key1
      res[0].data == val1

      res[1].key.get == key2
      res[1].data == val2

      res[2].key.get == key3
      res[2].data == val3

  test "Key should query all keys without values":
    let
      q = DbQuery(key: key1, value: false)

    ds.put(key1, val1).tryGet
    ds.put(key2, val2).tryGet
    ds.put(key3, val3).tryGet

    let
      (handle, iter) = ds.query(q).tryGet
      res = iter.mapIt(it.tryGet())
      
    check:
      res.len == 3
      res[0].key.get == key1
      res[0].data.len == 0

      res[1].key.get == key2
      res[1].data.len == 0

      res[2].key.get == key3
      res[2].data.len == 0


  test "Key should not query parent":
    let
      q = DbQuery(key: key1)

    ds.put(key1, val1).tryGet
    ds.put(key2, val2).tryGet
    ds.put(key3, val3).tryGet

    let
      (handle, iter) = ds.query(q).tryGet
      res = iter.mapIt(it.tryGet())

    check:
      res.len == 2
      res[0].key.get == key2
      res[0].data == val2

      res[1].key.get == key3
      res[1].data == val3

  test "Key should all list all keys at the same level":
    let
      queryKey = Key.init("/a").tryGet
      q = DbQuery(key: key1)

    ds.put(key1, val1).tryGet
    ds.put(key2, val2).tryGet
    ds.put(key3, val3).tryGet

    var
      (handle, iter) = ds.query(q).tryGet
      res = iter.mapIt(it.tryGet())

    res.sort do (a, b: DbQueryResponse) -> int:
      cmp($a.key.get, $b.key.get)

    check:
      res.len == 3
      res[0].key.get == key1
      res[0].data == val1

      res[1].key.get == key2
      res[1].data == val2

      res[2].key.get == key3
      res[2].data == val3

  test "Should apply limit":
    let
      key = Key.init("/a").tryGet
      q = DbQuery(key: key1, value: false)

    for i in 0..<100:
      let
        key = KeyId.new $Key.init(key, Key.init("/" & $i).tryGet).tryGet
        val = ("val " & $i).toBytes

      ds.put(key, val).tryGet

    let
      (handle, iter) = ds.query(q).tryGet
      res = iter.mapIt(it.tryGet())

    check:
      res.len == 10

  test "Should not apply offset":
    let
      key = Key.init("/a").tryGet
      keyId = KeyId.new $key
      q = DbQuery(key: KeyId.new $key, offset: 90)

    for i in 0..<100:
      let
        key = KeyId.new $Key.init(key, Key.init("/" & $i).tryGet).tryGet
        val = DataBuffer.new("val " & $i)

      ds.put(keyId, val).tryGet

    let
      (handle, iter) = ds.query(q).tryGet
      res = iter.mapIt(it.tryGet())

    check:
      res.len == 10


  test "Should not apply offset and limit":
    let
      key = Key.init("/a").tryGet
      keyId = KeyId.new $key
      q = DbQuery(key: keyId, offset: 95, limit: 5)

    for i in 0..<100:
      let
        key = KeyId.new $Key.init(key, Key.init("/" & $i).tryGet).tryGet
        val = DataBuffer.new("val " & $i)

      ds.put(key, val).tryGet

    let
      (handle, iter) = ds.query(q).tryGet
      res = iter.mapIt(it.tryGet())

    check:
      res.len == 5

    for i in 0..<res.high:
      let
        val = ("val " & $(i + 95)).toBytes
        key = Key.init(key, Key.init("/" & $(i + 95)).tryGet).tryGet

      check:
        res[i].key.get == key
        res[i].data == val


    test "Should apply sort order - descending":
      let
        key = Key.init("/a").tryGet
        q = DbQuery(key: key, sort: SortOrder.Descending)

      var kvs: seq[DbQueryResponse]
      for i in 0..<100:
        let
          k = KeyId.new $Key.init(key, Key.init("/" & $i).tryGet).tryGet
          val = DataBuffer.new ("val " & $i)

        kvs.add((k.some, val))
        ds.put(k, val).tryGet

      # lexicographic sort, as it comes from the backend
      kvs.sort do (a, b: DbQueryResponse) -> int:
        cmp($a.key.get, $b.key.get)

      kvs = kvs.reversed
      let
        (handle, iter) = ds.query(q).tryGet
        res = iter.mapIt(it.tryGet())

      check:
        res.len == 100

      for i, r in res[1..^1]:
        check:
          res[i].key.get == kvs[i].key.get
          res[i].data == kvs[i].data
