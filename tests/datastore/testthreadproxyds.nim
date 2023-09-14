import std/options
import std/sequtils
import std/os
import std/cpuinfo
import std/algorithm
import std/importutils

import pkg/asynctest
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils
import pkg/taskpools
import pkg/questionable/results

import pkg/datastore/sql
import pkg/datastore/fsds
import pkg/datastore/threads/threadproxyds {.all.}

import ./dscommontests
import ./querycommontests

suite "Test Basic ThreadDatastore with SQLite":

  var
    sqlStore: Datastore
    ds: ThreadDatastore
    taskPool: Taskpool
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  setupAll:
    sqlStore = SQLiteDatastore.new(Memory).tryGet()
    taskPool = Taskpool.new(countProcessors() * 2)
    ds = ThreadDatastore.new(sqlStore, taskPool).tryGet()

  teardownAll:
    (await ds.close()).tryGet()
    taskPool.shutdown()

  basicStoreTests(ds, key, bytes, otherBytes)

# suite "Test Basic ThreadDatastore with fsds":

#   let
#     path = currentSourcePath() # get this file's name
#     basePath = "tests_data"
#     basePathAbs = path.parentDir / basePath
#     key = Key.init("/a/b").tryGet()
#     bytes = "some bytes".toBytes
#     otherBytes = "some other bytes".toBytes

#   var
#     fsStore: FSDatastore
#     ds: ThreadDatastore
#     taskPool: Taskpool

  queryTests(ds, true)
