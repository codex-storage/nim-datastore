import std/options
import std/sequtils
import std/os
import std/algorithm

import pkg/asynctest
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils
import pkg/taskpools

import pkg/datastore/memoryds
import pkg/datastore/threads/threadproxyds

import ./dscommontests
# import ./querycommontests

import pretty


# suite "Test Basic ThreadProxyDatastore":
#   var
#     sds: ThreadDatastore
#     mem: MemoryDatastore
#     key1: Key
#     data: seq[byte]
#     taskPool: Taskpool

#   setupAll:
#     mem = MemoryDatastore.new()
#     taskPool = TaskPool.new(3)
#     sds = ThreadDatastore.new(mem, taskPool).expect("should work")
#     key1 = Key.init("/a").tryGet
#     data = "value for 1".toBytes()

#   test "check put":
#     echo "\n\n=== put ==="
#     let res1 = await sds.put(key1, data)
#     print "res1: ", res1

#   test "check get":
#     echo "\n\n=== get ==="
#     let res2 = await sds.get(key1)
#     check res2.get() == data
#     var val = ""
#     for c in res2.get():
#       val &= char(c)

#     print "get res2: ", $val

#     # echo "\n\n=== put cancel ==="
#     # # let res1 = await sds.put(key1, "value for 1".toBytes())
#     # let res3 = sds.put(key1, "value for 1".toBytes())
#     # res3.cancel()
#     # # print "res3: ", res3

suite "Test Basic ThreadProxyDatastore":

  var
    memStore: MemoryDatastore
    ds: ThreadDatastore
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes
    taskPool: Taskpool

  setupAll:
    memStore = MemoryDatastore.new()
    taskPool = Taskpool.new(2)
    ds = ThreadDatastore.new(memStore, taskPool).tryGet()

  teardownAll:
    (await memStore.close()).get()

  basicStoreTests(ds, key, bytes, otherBytes)

# suite "Test Query":
#   var
#     mem: MemoryDatastore
#     sds: ThreadProxyDatastore

#   setup:
#     mem = MemoryDatastore.new()
#     sds = newThreadProxyDatastore(mem).expect("should work")

#   queryTests(sds, false)

