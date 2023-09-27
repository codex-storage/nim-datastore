import std/options
import std/sequtils
import std/algorithm
import std/locks
import std/os
import pkg/stew/byteutils
import pkg/unittest2
import pkg/questionable
import pkg/questionable/results
import pkg/datastore/key

include ../../datastore/threads/databuffer

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
      check string.fromBytes(msg.toSeq()) == "hello world"
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
  var
    k1: Key
    k2: Key
    a: DataBuffer
    b: DataBuffer
    c: DataBuffer

  setup:
    k1 = Key.init("/a/b").tryGet()
    k2 = Key.init("/a").tryGet()
    a = DataBuffer.new($k1)
    b = DataBuffer.new($Key.init("/a/b").tryGet())
    c = DataBuffer.new($k2)

  test "creation":
    let db = DataBuffer.new("abc")
    check db[].size == 3
    check db[].buf[0].char == 'a'
    check db[].buf[1].char == 'b'
    check db[].buf[2].char == 'c'

  test "equality":
    check a == b

  test "toString":
    check $a == "/a/b"

  test "index":
    check c[0] == '/'.byte
    check c[1] == 'a'.byte
    expect IndexDefect:
      check c[2] == 'c'.byte

  test "hash":
    check a.hash() == b.hash()

  test "hashes differ":
    check a.hash() != c.hash()

  test "key conversion":
    check Key.init($a).tryGet == k1

  test "seq conversion":
    check string.fromBytes(a.toSeq()) == "/a/b"

  test "seq conversion":
    check a.toSeq() == "/a/b".toBytes

  test "basic null terminate test":
    let cstr = DataBuffer.new("test", {dbNullTerminate})
    check cstr.len() == 4
    check cstr.capacity() == 5
    check "test" == cstr.toString()

  test "basic clear test":
    let test = DataBuffer.new("test", {dbNullTerminate})
    test.clear()
    check "" == test.toString()
    test.setData("hi")
    check "hi" == test.toString()

  test "check openArray compare":
    assert a == toOpenArray(@"/a/b", 0, 3)

  test "basic openArray test":
    proc letters(val: openArray[char]): int =
      val.len()
    proc bytes(val: openArray[byte]): int =
      val.len()

    check a.toOpenArray(char).letters() == a.len()
    check a.toOpenArray(byte).bytes() == a.len()
    # check a.toOpenArray(char).bytes() == a.len()
    
  test "basic thread test":
    runBasicTest()
