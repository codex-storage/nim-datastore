import std/options
import std/sequtils
import std/os
import std/algorithm

import pkg/asynctest/unittest2
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore/memoryds
import pkg/datastore/threadproxyds

import ./dscommontests
import ./querycommontests

import pretty

suite "Test Basic ThreadProxyDatastore":
  var
    sds: ThreadProxyDatastore
    mem: MemoryDatastore
    res: ThreadProxyDatastore
    key1: Key
    data: seq[byte]

  setupAll:
    mem = MemoryDatastore.new()
    sds = newSharedDataStore(mem).expect("should work")
    key1 = Key.init("/a").tryGet
    data = "value for 1".toBytes()

  test "check put":
    echo "\n\n=== put ==="
    let res1 = await sds.put(key1, data)
    print "res1: ", res1

  test "check get":
    echo "\n\n=== get ==="
    let res2 = await sds.get(key1)
    check res2.get() == data
    var val = ""
    for c in res2.get():
      val &= char(c)
    print "get res2: ", $val

    # echo "\n\n=== put cancel ==="
    # # let res1 = await sds.put(key1, "value for 1".toBytes())
    # let res3 = sds.put(key1, "value for 1".toBytes())
    # res3.cancel()
    # # print "res3: ", res3

suite "Test Basic ThreadProxyDatastore":

  var
    memStore: MemoryDatastore
    ds: ThreadProxyDatastore
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  setupAll:
    memStore = MemoryDatastore.new()
    ds = newSharedDataStore(memStore).expect("should work")

  teardownAll:
    (await memStore.close()).get()

  basicStoreTests(ds, key, bytes, otherBytes)

# suite "Test Query":
#   let
#     path = currentSourcePath() # get this file's name
#     basePath = "tests_data"
#     basePathAbs = path.parentDir / basePath

#   var
#     ds: FSDatastore

#   setup:
#     removeDir(basePathAbs)
#     require(not dirExists(basePathAbs))
#     createDir(basePathAbs)

#     ds = FSDatastore.new(root = basePathAbs, depth = 5).tryGet()

#   teardown:

#     removeDir(basePathAbs)
#     require(not dirExists(basePathAbs))

#   queryTests(ds, false)
