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

suite "Test Basic SQLiteDatastore":

  let
    ds = SQLiteDatastore.new(Memory).tryGet()
    key = Key.init("a:b/c/d:e").tryGet().id()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  teardown:
    ds.close().tryGet()

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
    check:
      not await (key in ds)

  test "put batch":
    var
      batch: seq[BatchEntry]

    for k in 0..<100:
      batch.add((Key.init(key.id, $k).tryGet, @[k.byte]))

    ds.put(batch).tryGet

    for k in batch:
      check: ds.has(k.key).tryGet

  test "delete batch":
    var
      batch: seq[Key]

    for k in 0..<100:
      batch.add(Key.init(key.id, $k).tryGet)

    ds.delete(batch).tryGet

    for k in batch:
      check: not ds.has(k).tryGet

  test "handle missing key":
    let key = Key.init("/missing/key").tryGet()

    expect(DatastoreKeyNotFound):
      discard ds.get(key).tryGet() # non existing key
