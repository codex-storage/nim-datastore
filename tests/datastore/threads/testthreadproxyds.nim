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
import pkg/threading/atomics

import pkg/datastore/sql
import pkg/datastore/fsds
import pkg/datastore/threads/threadproxy

import ../dscommontests
import ../querycommontests

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
      ds: SQLiteDatastore
      key = Key.init("/a/b").tryGet()
      bytes = "some bytes".toBytes
      otherBytes = "some other bytes".toBytes

    setupAll:
      ds = SQLiteDatastore.new(Memory, tp=taskPool).tryGet()

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
      ds: SQLiteDatastore

    setup:
      ds = SQLiteDatastore.new(Memory, tp = taskPool).tryGet()

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
    ds: FSDatastore

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    ds = FSDatastore.new(root=basePathAbs, tp=taskPool, depth=5).tryGet()

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
    ds: FSDatastore

  setup:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    ds = FSDatastore.new(root=basePathAbs, tp = taskPool, depth=5).tryGet()

  teardown:
    GC_fullCollect()
    (await ds.close()).tryGet()

    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  queryTests(ds, false)

for i in 1..N:
  suite "Test ThreadDatastore cancelations":

    privateAccess(SQLiteDatastore) # expose private fields
    privateAccess(ThreadProxy) # expose private fields
    privateAccess(TaskCtx) # expose private fields

    var sds: SQLiteDatastore

    setupAll:
      sds = SQLiteDatastore.new(Memory, tp = taskPool).tryGet()

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
      dispatchTask(sds.db, signal):
        sds.db.tp.spawn cancelTestTask(ctx)

      check:
        ctx[].res.isErr == true
        ctx[].cancelled == true
        ctx[].running == false

    test "Should cancel future":

      var
        signal = ThreadSignalPtr.new().tryGet()
        ms {.global.}: MutexSignal
        flag {.global.}: Atomic[bool]
        futFreed {.global.}: Atomic[bool]
        ready {.global.}: Atomic[bool]

      ms.init()

      type
        FutTestObj = object
          val: int
        TestValue = object
        ThreadTestInt = (TestValue, )

      proc `=destroy`(obj: var TestValue) =
        # echo "destroy TestObj!"
        flag.store(true)

      proc `=destroy`(obj: var FutTestObj) =
        # echo "destroy FutTestObj!"
        futFreed.store(true)

      proc wait(flag: var Atomic[bool], name = "task") =
        # echo "wait for " & name & " to be ready..."
        # defer: echo ""
        for i in 1..100:
          # stdout.write(".")
          if flag.load() == true: 
            return
          os.sleep(10)
        raise newException(Defect, "timeout")

      proc errorTestTask(ctx: TaskCtx[ThreadTestInt]) {.gcsafe, nimcall.} =
        executeTask(ctx):
          # echo "task:exec"
          discard ctx[].signal.fireSync()
          ready.store(true)
          ms.wait()
          echo "task context memory: ", ctx[]
          (?!ThreadTestInt).ok(default(ThreadTestInt))

      proc runTestTask() {.async.} =
        let obj = FutTestObj(val: 42)
        await sleepAsync(1.milliseconds)
        try:
          let ctx = newTaskCtx(ThreadTestInt, signal=signal)
          dispatchTask(sds.db, signal):
            sds.db.tp.spawn errorTestTask(ctx)
            ready.wait()
            # echo "raise error"
            raise newException(ValueError, "fake error")
        finally:
          # echo "fut FutTestObj: ", obj
          assert obj.val == 42 # need to force future to keep ref here
      try:
        block:
          await runTestTask()
      except CatchableError as exc:
        # echo "caught: ", $exc
        discard
      finally:
        # echo "finish"
        check ready.load() == true
        GC_fullCollect()
        futFreed.wait("futFreed")
        echo "future freed it's mem!"
        check futFreed.load() == true

        ms.fire()
        flag.wait("flag")
        check flag.load() == true
