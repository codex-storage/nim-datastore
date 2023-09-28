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
      ds = ThreadDatastore.new(sqlStore, tp = taskPool).tryGet()

    teardown:
      GC_fullCollect()

    teardownAll:
      (await ds.close()).tryGet()

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

suite "Test Query ThreadDatastore with fsds":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath

  var
    fsStore: FSDatastore[KeyId, DataBuffer]
    ds: ThreadDatastore[FSDatastore[KeyId, DataBuffer]]

  setup:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    fsStore = newFSDatastore[KeyId, DataBuffer](root = basePathAbs, depth = 5).tryGet()
    ds = ThreadDatastore.new(fsStore, tp = taskPool).tryGet()

  teardown:
    GC_fullCollect()
    (await ds.close()).tryGet()

    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  queryTests(ds, false)

suite "Test ThreadDatastore cancelations":
  var
    sqlStore: SQLiteBackend[KeyId,DataBuffer]
    sds: ThreadDatastore[SQLiteBackend[KeyId, DataBuffer]]

  privateAccess(ThreadDatastore) # expose private fields
  privateAccess(TaskCtx) # expose private fields

  setupAll:
    sqlStore = newSQLiteBackend[KeyId, DataBuffer](Memory).tryGet()
    sds = ThreadDatastore.new(sqlStore, tp = taskPool).tryGet()

  teardown:
    GC_fullCollect() # run full collect after each test

  test "Should monitor signal and cancel":
    var
      signal = ThreadSignalPtr.new().tryGet()

    proc cancelTestTask(ctx: TaskCtx[bool]) {.gcsafe.} =
      executeTask(ctx):
        (?!bool).ok(true)

    let ctx = newTaskCtx(bool, signal=signal)
    ctx[].cancelled = true
    dispatchTask(sds, signal):
      sds.tp.spawn cancelTestTask(ctx)

    check:
      ctx[].res.isErr == true
      ctx[].cancelled == true
      ctx[].running == false

  test "Should cancel future":

    var
      signal = ThreadSignalPtr.new().tryGet()
      ms {.global.}: MutexSignal
      flag {.global.}: int = 0

    ms.init()

    type
      TestValue = object
      ThreadTestInt = (TestValue, )

    proc `=destroy`(obj: var TestValue) =
      echo "destroy TestObj!"
      flag = 10

    proc errorTestTask(ctx: TaskCtx[ThreadTestInt]) {.gcsafe, nimcall.} =
      executeTask(ctx):
        discard ctx[].signal.fireSync()
        ms.wait()
        (?!ThreadTestInt).ok(default(ThreadTestInt))

    proc runTestTask() {.async.} =

      let ctx = newTaskCtx(ThreadTestInt, signal=signal)
      dispatchTask(sds, signal):
        sds.tp.spawn errorTestTask(ctx)
      
      echo "raise error"
      raise newException(ValueError, "fake error")

    try:
      await runTestTask()
    except CatchableError as exc:
      echo "caught: ", $exc
    finally:
      echo "finish"
      ms.fire()
      os.sleep(10)
      check flag == 10

