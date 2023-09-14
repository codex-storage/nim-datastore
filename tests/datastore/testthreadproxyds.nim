import std/options
import std/sequtils
import std/os
import std/cpuinfo
import std/algorithm

import pkg/asynctest
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils
import pkg/taskpools

import pkg/datastore/sql
import pkg/datastore/threads/threadproxyds

import ./dscommontests
import ./querycommontests

suite "Test Basic ThreadDatastore":

  var
    memStore: Datastore
    ds: ThreadDatastore
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes
    taskPool: Taskpool

  setupAll:
    memStore = SQLiteDatastore.new(Memory).tryGet()
    taskPool = Taskpool.new(countProcessors() * 2)
    ds = ThreadDatastore.new(memStore, taskPool).tryGet()

  teardownAll:
    (await ds.close()).tryGet()
    taskPool.shutdown()

  basicStoreTests(ds, key, bytes, otherBytes)

suite "Test Query ThreadDatastore":
  var
    mem: Datastore
    ds: ThreadDatastore
    taskPool: Taskpool

  setup:
    taskPool = Taskpool.new(countProcessors() * 2)
    mem = SQLiteDatastore.new(Memory).tryGet()
    ds = ThreadDatastore.new(mem, taskPool).tryGet()

  teardown:
    (await ds.close()).tryGet()
    taskPool.shutdown()

  queryTests(ds, true)
