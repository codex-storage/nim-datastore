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
import pkg/threading/smartptrs

import pkg/datastore/fsds
import pkg/datastore/sql/sqliteds
import pkg/datastore/threads/threadproxyds {.all.}

import ./dscommontests
import ./querycommontests

const
  NumThreads = 20 # IO threads aren't attached to CPU count
  ThreadTestLoops {.intdefine.} = 1
  N = ThreadTestLoops
  ThreadTestInnerLoops {.intdefine.} = 1
  M = ThreadTestInnerLoops 

var
  taskPool: Taskpool = Taskpool.new(NumThreads)

for i in 1..N:
  suite "Test Basic ThreadDatastore with SQLite " & $i:

    var
      sqlStore: SQLiteBackend[KeyId, DataBuffer]
      ds: ThreadDatastore[SQLiteBackend[KeyId, DataBuffer]]
      key = Key.init("/a/b").tryGet()
      bytes = "some bytes".toBytes
      otherBytes = "some other bytes".toBytes

    setupAll:
      sqlStore = newSQLiteBackend[KeyId, DataBuffer](Memory).tryGet()
      # taskPool = Taskpool.new(NumThreads)
      ds = ThreadDatastore.new(sqlStore, tp = taskPool).tryGet()

    teardown:
      GC_fullCollect()

    teardownAll:
      (await ds.close()).tryGet()
      # taskPool.shutdown()

    for i in 1..M:
      basicStoreTests(ds, key, bytes, otherBytes)
  GC_fullCollect()


for i in 1..N:
  suite "Test Query ThreadDatastore with SQLite " & $i:

    var
      sqlStore: SQLiteBackend[KeyId, DataBuffer]
      # taskPool: Taskpool
      ds: ThreadDatastore[SQLiteBackend[KeyId, DataBuffer]]

    setup:
      sqlStore = newSQLiteBackend[KeyId, DataBuffer](Memory).tryGet()
      # taskPool = Taskpool.new(NumThreads)
      ds = ThreadDatastore.new(sqlStore, tp = taskPool).tryGet()

    teardown:
      GC_fullCollect()

      (await ds.close()).tryGet()
      # taskPool.shutdown()

    for i in 1..M:
      queryTests(ds, true)
  GC_fullCollect()

suite "Test Basic ThreadDatastore with fsds":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  var
    fsStore: FSDatastore[KeyId, DataBuffer]
    ds: ThreadDatastore[FSDatastore[KeyId, DataBuffer]]
    taskPool: Taskpool

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    fsStore = newFSDatastore[KeyId, DataBuffer](root = basePathAbs, depth = 3).tryGet()
    ds = ThreadDatastore.new(fsStore, tp = taskPool).tryGet()

  teardown:
    GC_fullCollect()

  teardownAll:
    (await ds.close()).tryGet()

    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  basicStoreTests(ds, key, bytes, otherBytes)


# suite "Test Query ThreadDatastore with fsds":
#   let
#     path = currentSourcePath() # get this file's name
#     basePath = "tests_data"
#     basePathAbs = path.parentDir / basePath

#   var
#     fsStore: FSDatastore
#     ds: ThreadDatastore
#     taskPool: Taskpool

#   setup:
#     removeDir(basePathAbs)
#     require(not dirExists(basePathAbs))
#     createDir(basePathAbs)

#     fsStore = FSDatastore.new(root = basePathAbs, depth = 5).tryGet()
#     taskPool = Taskpool.new(NumThreads)
#     ds = ThreadDatastore.new(fsStore, withLocks = true, tp = taskPool).tryGet()

#   teardown:
#     GC_fullCollect()
#     (await ds.close()).tryGet()
#     taskPool.shutdown()

#     removeDir(basePathAbs)
#     require(not dirExists(basePathAbs))

#   queryTests(ds, false)

# suite "Test ThreadDatastore cancelations":
#   var
#     sqlStore: SQLiteBackend[KeyId,DataBuffer]
#     ds: ThreadDatastore
#     taskPool: Taskpool

#   privateAccess(ThreadDatastore) # expose private fields
#   privateAccess(TaskCtx) # expose private fields

#   setupAll:
#     sqlStore = newSQLiteBackend[KeyId, DataBuffer](Memory).tryGet()
#     taskPool = Taskpool.new(NumThreads)
#     ds = ThreadDatastore.new(sqlStore, tp = taskPool).tryGet()

#   teardown:
#     GC_fullCollect() # run full collect after each test

#   teardownAll:
#     (await ds.close()).tryGet()
#     taskPool.shutdown()

  # test "Should monitor signal and cancel":
  #   var
  #     signal = ThreadSignalPtr.new().tryGet()
  #     res = ThreadResult[void]()
  #     ctx = newSharedPtr(TaskCtxObj[void](signal: signal))
  #     fut = newFuture[void]("signalMonitor")
  #     threadArgs = (addr ctx, addr fut)
  #     thread: Thread[type threadArgs]

  #   proc threadTask(args: type threadArgs) =
  #     var (ctx, fut) = args
  #     proc asyncTask() {.async.} =
  #       let
  #         monitor = signalMonitor(ctx, fut[])

  #       await monitor

  #     waitFor asyncTask()

  #   createThread(thread, threadTask, threadArgs)
  #   ctx.cancelled = true
  #   check: ctx.signal.fireSync.tryGet

  #   joinThreads(thread)

  #   check: fut.cancelled
  #   check: ctx.signal.close().isOk
  #   fut = nil

  # test "Should monitor and not cancel":
  #   var
  #     signal = ThreadSignalPtr.new().tryGet()
  #     res = ThreadResult[void]()
  #     ctx = TaskCtx[void](
  #       ds: sqlStore,
  #       res: addr res,
  #       signal: signal)
  #     fut = newFuture[void]("signalMonitor")
  #     threadArgs = (addr ctx, addr fut)
  #     thread: Thread[type threadArgs]

  #   proc threadTask(args: type threadArgs) =
  #     var (ctx, fut) = args
  #     proc asyncTask() {.async.} =
  #       let
  #         monitor = signalMonitor(ctx, fut[])

  #       await monitor

  #     waitFor asyncTask()

  #   createThread(thread, threadTask, threadArgs)
  #   ctx.cancelled = false
  #   check: ctx.signal.fireSync.tryGet

  #   joinThreads(thread)

  #   check: not fut.cancelled
  #   check: ctx.signal.close().isOk
  #   fut = nil
