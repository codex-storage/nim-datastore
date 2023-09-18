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
import pkg/datastore/threads/threadproxyds {.all.}

import ./dscommontests
import ./querycommontests

const NumThreads = 200 # IO threads aren't attached to CPU count

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
    taskPool = Taskpool.new(NumThreads)
    ds = ThreadDatastore.new(sqlStore, tp = taskPool).tryGet()

  teardownAll:
    (await ds.close()).tryGet()
    taskPool.shutdown()

  basicStoreTests(ds, key, bytes, otherBytes)

suite "Test Query ThreadDatastore with SQLite":

  var
    sqlStore: Datastore
    ds: ThreadDatastore
    taskPool: Taskpool
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  setup:
    sqlStore = SQLiteDatastore.new(Memory).tryGet()
    taskPool = Taskpool.new(NumThreads)
    ds = ThreadDatastore.new(sqlStore, tp = taskPool).tryGet()

  teardown:
    (await ds.close()).tryGet()
    taskPool.shutdown()

  queryTests(ds, true)

suite "Test Basic ThreadDatastore with fsds":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  var
    fsStore: FSDatastore
    ds: ThreadDatastore
    taskPool: Taskpool

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    fsStore = FSDatastore.new(root = basePathAbs, depth = 3).tryGet()
    taskPool = Taskpool.new(NumThreads)
    ds = ThreadDatastore.new(fsStore, withLocks = true, tp = taskPool).tryGet()

  teardownAll:
    (await ds.close()).tryGet()
    taskPool.shutdown()

    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  basicStoreTests(fsStore, key, bytes, otherBytes)

suite "Test Query ThreadDatastore with fsds":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath

  var
    fsStore: FSDatastore
    ds: ThreadDatastore
    taskPool: Taskpool

  setup:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    fsStore = FSDatastore.new(root = basePathAbs, depth = 5).tryGet()
    taskPool = Taskpool.new(NumThreads)
    ds = ThreadDatastore.new(fsStore, withLocks = true, tp = taskPool).tryGet()

  teardown:
    (await ds.close()).tryGet()
    taskPool.shutdown()

    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  queryTests(ds, false)

suite "Test ThreadDatastore cancelations":
  var
    sqlStore: Datastore
    ds: ThreadDatastore
    taskPool: Taskpool
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  privateAccess(ThreadDatastore) # expose private fields
  privateAccess(TaskCtx) # expose private fields

  setupAll:
    sqlStore = SQLiteDatastore.new(Memory).tryGet()
    taskPool = Taskpool.new(NumThreads)
    ds = ThreadDatastore.new(sqlStore, tp = taskPool).tryGet()

  test "Should monitor signal and cancel":
    var
      signal = ThreadSignalPtr.new().tryGet()
      ctx = TaskCtx[void](
        ds: addr sqlStore,
        signal: signal)
      fut = newFuture[void]("signalMonitor")
      threadArgs: (ptr TaskCtx, ptr Future[void]) = (unsafeAddr ctx[], addr fut)

    var
      thread: Thread[type threadArgs]

    proc threadTask(args: type threadArgs) =
      var (ctx, fut) = args
      proc asyncTask() {.async.} =
        let
          monitor = signalMonitor(ctx, fut[])

        await monitor

      waitFor asyncTask()

    createThread(thread, threadTask, threadArgs)
    ctx.cancelled = true
    check: ctx.signal.fireSync.tryGet

    joinThreads(thread)

    check: fut.cancelled
    check: ctx.signal.close().isOk

  test "Should monitor and not cancel":
    var
      signal = ThreadSignalPtr.new().tryGet()
      res = ThreadResult[void]()
      ctx = TaskCtx[void](
        ds: addr sqlStore,
        res: addr res,
        signal: signal)
      fut = newFuture[void]("signalMonitor")
      threadArgs = (addr ctx, addr fut)

    var
      thread: Thread[type threadArgs]

    proc threadTask(args: type threadArgs) =
      var (ctx, fut) = args
      proc asyncTask() {.async.} =
        let
          monitor = signalMonitor(ctx, fut[])

        await monitor

      waitFor asyncTask()

    createThread(thread, threadTask, threadArgs)
    ctx.cancelled = false
    check: ctx.signal.fireSync.tryGet

    joinThreads(thread)

    check: not fut.cancelled
    check: ctx.signal.close().isOk
