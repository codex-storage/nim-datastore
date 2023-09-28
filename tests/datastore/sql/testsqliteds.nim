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

import ../backendCommonTests


suite "Test Basic SQLiteDatastore":
  let
    ds = newSQLiteBackend[string, seq[byte]](path=Memory).tryGet()
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

  testBasicBackend(ds, key, bytes, otherBytes, batch)

suite "Test DataBuffer SQLiteDatastore":
  let
    ds = newSQLiteBackend[KeyId, DataBuffer](Memory).tryGet()
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

  testBasicBackend(ds, key, bytes, otherBytes, batch)

suite "queryTests":

  let
    dsNew = proc(): SQLiteBackend[KeyId, DataBuffer] =
      newSQLiteBackend[KeyId, DataBuffer](Memory).tryGet()
    key1 = KeyId.new "/a"
    key2 = KeyId.new "/a/b"
    key3 = KeyId.new "/a/b/c"
    val1 = DataBuffer.new "value for 1"
    val2 = DataBuffer.new "value for 2"
    val3 = DataBuffer.new "value for 3"

  queryTests(dsNew, key1, key2, key3, val1, val2, val3, extended=true)
