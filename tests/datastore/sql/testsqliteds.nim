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
  ds: SQLiteDatastore,
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
    ds = SQLiteDatastore.new(Memory).tryGet()
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
    ds = SQLiteDatastore.new(Memory).tryGet()
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
