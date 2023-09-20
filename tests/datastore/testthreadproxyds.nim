import std/options
import std/sequtils
import std/os
import std/algorithm

import pkg/asynctest
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore/memoryds
import pkg/datastore/threadproxyds

import ./dscommontests
import ./querycommontests

import pretty


proc testThreadProxy() =
  suite "Test Basic ThreadProxyDatastore":
    var
      sds: ThreadProxyDatastore
      mem: MemoryDatastore
      key1: Key
      data: seq[byte]

    setupAll:
      mem = MemoryDatastore.new()
      sds = newThreadProxyDatastore(mem).expect("should work")
      key1 = Key.init("/a").tryGet
      data = "value for 1".toBytes()

    teardownAll:
      let res = await sds.close()
      res.get()
      echo "teardown done"

    test "check put":
      # echo "\n\n=== put ==="
      let res1 = await sds.put(key1, data)
      # echo "res1: ", res1.repr
      check res1.isOk

    test "check get":
      # echo "\n\n=== get ==="
      # echo "get send key: ", key1.repr
      let res2 = await sds.get(key1)
      # echo "get key post: ", key1.repr
      # echo "get res2: ", res2.repr
      # echo res2.get() == data
      var val = ""
      for c in res2.get():
        val &= char(c)
      # print "get res2: ", $val

proc testThreadProxyBasics() =
  suite "Test Basics":
    var
      mem = MemoryDatastore.new()
      sds = newThreadProxyDatastore(mem).expect("should work")

    let
      key = Key.init("/a/b").tryGet()
      bytes = "some bytes".toBytes
      otherBytes = "some other bytes".toBytes

      # echo "\n\n=== put cancel ==="
      # # let res1 = await sds.put(key1, "value for 1".toBytes())
      # let res3 = sds.put(key1, "value for 1".toBytes())
      # res3.cancel()
      # # print "res3: ", res3

    basicStoreTests(sds, key, bytes, otherBytes)

proc testThreadProxyQuery() =
  suite "Test Query":
    var
      mem: MemoryDatastore
      sds: ThreadProxyDatastore

    setup:
      mem = MemoryDatastore.new()
      sds = newThreadProxyDatastore(mem).expect("should work")

    queryTests(sds, false)

    test "query iter fails":

      expect FutureDefect:
        let q = Query.init(key1)

        (await sds.put(key1, val1)).tryGet
        (await sds.put(key2, val2)).tryGet
        (await sds.put(key3, val3)).tryGet

        let
          iter = (await sds.query(q)).tryGet
          res = (await allFinished(toSeq(iter)))
            .mapIt(it.read.tryGet)
            .filterIt(it.key.isSome)

        check res.len() > 0

when isMainModule:
  for i in 1..100:
    testThreadProxy()
    testThreadProxyBasics()
    # testThreadProxyQuery()
else:
  testThreadProxy()
  testThreadProxyBasics()
  testThreadProxyQuery()

# GC_fullCollect() # this fails due to MemoryStore already being freed...
