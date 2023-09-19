import std/options
import std/sequtils
import std/os
import std/cpuinfo
import std/algorithm
import std/importutils

import pkg/asynctest
import pkg/chronos
import pkg/chronos/threadsync
import pkg/stew/results
import pkg/stew/byteutils
import pkg/taskpools
import pkg/questionable/results
import pkg/chronicles

import pkg/datastore/sql
import pkg/datastore/fsds
import pkg/datastore/memoryds
import pkg/datastore/threads/threadproxyds {.all.}

import ./dscommontests
import ./querycommontests

const NumThreads = 200 # IO threads aren't attached to CPU count

proc testBasicMemory() =

  suite "Test Basic ThreadDatastore with SQLite":

    var
      memStore: Datastore
      ds: ThreadDatastore
      taskPool: Taskpool
      key = Key.init("/a/b").tryGet()
      bytes = "some bytes".toBytes
      otherBytes = "some other bytes".toBytes

    setupAll:
      memStore = MemoryDatastore.new()
      taskPool = Taskpool.new(NumThreads)
      ds = ThreadDatastore.new(memStore, tp = taskPool).tryGet()

    teardownAll:
      (await ds.close()).tryGet()
      taskPool.shutdown()

    basicStoreTests(ds, key, bytes, otherBytes)
    GC_fullCollect()

for i in 1..1000:
  testBasicMemory()
  GC_fullCollect()
