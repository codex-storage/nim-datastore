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

proc threadTest() =
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

    test "check put":
      # echo "\n\n=== put ==="
      let res1 = await sds.put(key1, data)
      print "res1: ", res1
      check res1.isOk
      # GC_fullCollect()

proc main() =
  threadTest()
  # GC_fullCollect()

main()
# GC_fullCollect()


#   test "check get":
#     # echo "\n\n=== get ==="
#     let res2 = await sds.get(key1)
#     check res2.get() == data
#     var val = ""
#     for c in res2.get():
#       val &= char(c)
#     # print "get res2: ", $val

#     # echo "\n\n=== put cancel ==="
#     # # let res1 = await sds.put(key1, "value for 1".toBytes())
#     # let res3 = sds.put(key1, "value for 1".toBytes())
#     # res3.cancel()
#     # # print "res3: ", res3

# suite "Test Basic ThreadProxyDatastore":

#   var
#     memStore: MemoryDatastore
#     ds: ThreadProxyDatastore
#     key = Key.init("/a/b").tryGet()
#     bytes = "some bytes".toBytes
#     otherBytes = "some other bytes".toBytes

#   setupAll:
#     memStore = MemoryDatastore.new()
#     ds = newThreadProxyDatastore(memStore).expect("should work")

#   teardownAll:
#     (await memStore.close()).get()

#   basicStoreTests(ds, key, bytes, otherBytes)

# suite "Test Query":
#   var
#     mem: MemoryDatastore
#     sds: ThreadProxyDatastore

#   setup:
#     mem = MemoryDatastore.new()
#     sds = newThreadProxyDatastore(mem).expect("should work")

#   queryTests(sds, false)

#   test "query iter fails":

#     expect FutureDefect:
#       let q = Query.init(key1)

#       (await sds.put(key1, val1)).tryGet
#       (await sds.put(key2, val2)).tryGet
#       (await sds.put(key3, val3)).tryGet

#       let
#         iter = (await sds.query(q)).tryGet
#         res = (await allFinished(toSeq(iter)))
#           .mapIt( it.read.tryGet )
#           .filterIt( it.key.isSome )
      
#       check res.len() > 0

