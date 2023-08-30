import std/options
import std/sequtils
import std/algorithm
import std/locks
import std/os
import pkg/stew/byteutils
import pkg/unittest2
import pkg/questionable
import pkg/questionable/results

include ../../datastore/databuffer

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

      echo "thread1: sending: ", myBytes
      echo "mybytes2: ", myBytes2

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
      echo "thread2: received: ", msg
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

  setup:
    let k1 {.used.} = Key.init("/a/b").get()
    let k2 {.used.} = Key.init("/a").get()
    let a {.used.} = KeyBuffer.new(k1)
    let b {.used.} = KeyBuffer.new(Key.init("/a/b").get)
    let c {.used.} = KeyBuffer.new(k2)

  test "creation":
    let db = DataBuffer.new("abc")
    check db[].size == 3
    check db[].buf[0].char == 'a'
    check db[].buf[1].char == 'b'
    check db[].buf[2].char == 'c'
  test "equality":
    check a == b
  test "toString":
    check a.toString() == "/a/b"
  test "hash":
    check a.hash() == b.hash()
  test "hashes differ":
    check a.hash() != c.hash()
  test "key conversion":
    check a.toKey().get() == k1
  test "seq conversion":
    check a.toSeq(char) == @"/a/b"
  test "seq conversion":
    check a.toSeq(byte) == "/a/b".toBytes

  test "basic thread test":
    runBasicTest()

