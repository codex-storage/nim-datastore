import std/options
import std/sequtils
from std/algorithm import sort, reversed

import pkg/asynctest
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore

template queryTests*(ds: Datastore, extended = true) {.dirty.} =
  test "Key should query all key and all it's children":
    let
      key1 = Key.init("/a").tryGet
      key2 = Key.init("/a/b").tryGet
      key3 = Key.init("/a/b/c").tryGet
      val1 = "value for 1".toBytes
      val2 = "value for 2".toBytes
      val3 = "value for 3".toBytes

      q = Query.init(key1)

    (await ds.put(key1, val1)).tryGet
    (await ds.put(key2, val2)).tryGet
    (await ds.put(key3, val3)).tryGet

    let
      iter = (await ds.query(q)).tryGet
      res = await allFinished(toSeq(iter))

    check:
      res.len == 4
      res[0].read.tryGet.key.get == key1
      res[0].read.tryGet.data == val1

      res[1].read.tryGet.key.get == key2
      res[1].read.tryGet.data == val2

      res[2].read.tryGet.key.get == key3
      res[2].read.tryGet.data == val3

    (await iter.dispose()).tryGet

  test "Key should not query parent":
    let
      key1 = Key.init("/a").tryGet
      key2 = Key.init("/a/b").tryGet
      key3 = Key.init("/a/b/c").tryGet
      val1 = "value for 1".toBytes
      val2 = "value for 2".toBytes
      val3 = "value for 3".toBytes

      q = Query.init(key2)

    (await ds.put(key1, val1)).tryGet
    (await ds.put(key2, val2)).tryGet
    (await ds.put(key3, val3)).tryGet

    let
      iter = (await ds.query(q)).tryGet
      res = await allFinished(toSeq(iter))

    check:
      res.len == 3
      res[0].read.tryGet.key.get == key2
      res[0].read.tryGet.data == val2

      res[1].read.tryGet.key.get == key3
      res[1].read.tryGet.data == val3

    (await iter.dispose()).tryGet

  if extended:
    test "Should apply limit":

      let
        key = Key.init("/a").tryGet
        q = Query.init(key, limit = 10)

      for i in 0..<100:
        (await ds.put(Key.init(key, Key.init("/" & $i).tryGet).tryGet, ("val " & $i).toBytes)).tryGet

      let
        iter = (await ds.query(q)).tryGet
        res = await allFinished(toSeq(iter))

      check:
        res.len == 11

      (await iter.dispose()).tryGet

    test "Should not apply offset":
      let
        key = Key.init("/a").tryGet
        q = Query.init(key, offset = 90)

      for i in 0..<100:
        (await ds.put(Key.init(key, Key.init("/" & $i).tryGet).tryGet, ("val " & $i).toBytes)).tryGet

      let
        iter = (await ds.query(q)).tryGet
        res = await allFinished(toSeq(iter))

      check:
        res.len == 11

      (await iter.dispose()).tryGet

    test "Should not apply offset and limit":
      let
        key = Key.init("/a").tryGet
        q = Query.init(key, offset = 95, limit = 5)

      for i in 0..<100:
        (await ds.put(Key.init(key, Key.init("/" & $i).tryGet).tryGet, ("val " & $i).toBytes)).tryGet

      let
        iter = (await ds.query(q)).tryGet
        res = await allFinished(toSeq(iter))

      check:
        res.len == 6

      for i in 0..<res.high:
        let
          val = ("val " & $(i + 95)).toBytes
          key = Key.init(key, Key.init("/" & $(i + 95)).tryGet).tryGet

        check:
          res[i].read.tryGet.key.get == key
          res[i].read.tryGet.data == val

      (await iter.dispose()).tryGet

    test "Should apply sort order - descending":
      let
        key = Key.init("/a").tryGet
        q = Query.init(key, sort = SortOrder.Descending)

      var kvs: seq[QueryResponse]
      for i in 0..<100:
        let
          k = Key.init(key, Key.init("/" & $i).tryGet).tryGet
          val = ("val " & $i).toBytes

        kvs.add((k.some, val))
        (await ds.put(k, val)).tryGet

      kvs.sort do (a, b: QueryResponse) -> int:
        cmp(a.key.get.id, b.key.get.id)

      kvs = kvs.reversed
      let
        iter = (await ds.query(q)).tryGet
        res = await allFinished(toSeq(iter))

      check:
        res.len == 101

      for i, r in res[1..^1]:
        check:
          res[i].read.tryGet.key.get == kvs[i].key.get
          res[i].read.tryGet.data == kvs[i].data

      (await iter.dispose()).tryGet
