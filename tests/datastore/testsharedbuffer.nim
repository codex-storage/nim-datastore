import std/options
import std/sequtils
import std/algorithm
import std/locks
import std/os

import pkg/unittest2
import pkg/questionable
import pkg/questionable/results

include ../../datastore/databuffer

type
  AtomicFreed* = ptr int

proc newFreedValue*(val = 0): ptr int =
  result = cast[ptr int](alloc0(sizeof(int)))
  result[] = val

proc getFreedValue*(x: ptr int): int =
  atomicLoad(x, addr result, ATOMIC_ACQUIRE)

proc incrFreedValue*(x: ptr int): int =
  atomicAddFetch(x, 1, ATOMIC_ACQUIRE)

proc decrFreedValue*(x: ptr int): int =
  atomicSubFetch(x, 1, ATOMIC_ACQUIRE)


var
  shareVal: DataBuffer
  lock: Lock
  cond: Cond

var threads: array[2,Thread[int]]

proc thread1(val: int) {.thread.} =
  echo "thread1"
  {.cast(gcsafe).}:
    for i in 0..<val:
      os.sleep(20)
      var myBytes2 = DataBuffer.new()
      var myBytes = DataBuffer.new(@"hello world")
      myBytes2 = myBytes

      echo "thread1: sending: ", myBytes, " cnt: ", myBytes.unsafeGetAtomicCount()
      echo "mybytes2: ", myBytes2, " cnt: ", myBytes2.unsafeGetAtomicCount()

      shareVal = myBytes
      echo "thread1: sent, left over: ", $myBytes
      lock.withLock:
        signal(cond)
      os.sleep(10)

proc thread2(val: int) {.thread.} =
  echo "thread2"
  {.cast(gcsafe).}:
    for i in 0..<val:
      lock.withLock:
        wait(cond, lock)
      echo "thread2: receiving "
      let msg: DataBuffer = shareVal
      echo "thread2: received: ", msg, " cnt: ", msg.unsafeGetAtomicCount()
      check msg.toSeq(char) == @"hello world"
      # os.sleep(100)

proc runBasicTest() =
  echo "running"

  lock.initLock()
  cond.initCond()

  let n = 1
  createThread(threads[0], thread1, n)
  createThread(threads[1], thread2, n)

  joinThreads(threads)

suite "Share buffer test":

  test "basic test":
    runBasicTest()

