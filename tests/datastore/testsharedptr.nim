import std/options
import std/locks
import std/os
import pkg/stew/byteutils
import pkg/unittest2
import pkg/questionable
import pkg/questionable/results

include pkg/datastore/threads/sharedptr
include pkg/datastore/threads/databuffer


type
  TestObj = object
    val: ref int

  TestObjGen[T] = object
    val: ref T

proc `=destroy`(obj: var TestObj) =
  echo "test obj destroy"
  obj.val[] = 0

proc `=destroy`[T: int](obj: var TestObjGen[T]) =
  echo "test obj destroy"
  obj.val[] = 0

proc destroyTest(intref: ref int) =
  let a: SharedPtr[TestObj] = newSharedPtr(unsafeIsolate TestObj(val: intref))
  echo "a[]: ", a[]
  check a[].val[] == 10

proc runDestroyTest() =
  echo "\nintref setup:\n"
  let intref: ref int = new(ref int)
  intref[] = 10
  destroyTest(intref)
  check intref[] == 0

proc runDestroyOnReleaseTest() =
  echo "\nintref setup:\n"
  let intref: ref int = new(ref int)
  intref[] = 20
  var a: SharedPtr[TestObj] = newSharedPtr(unsafeIsolate TestObj(val: intref))
  try:
    echo "a[]: ", a[]
    check a[].val[] == 20
  finally:
    a.release()
    check intref[] == 0
    ## important a should be nil now!
    ## to prevent future decr's from occuring
    check a.isNil == true
    a.decr()
    a.release()


suite "Share buffer test":

  setup:
    var a1 {.used.}: SharedPtr[int]
    let a2 {.used.} = newSharedPtr(0)
    let a3 {.used.} = a2

  test "basics":
    echo "a1: ", $a1
    check $a1 == "nil\"\""
    check a1.isNil
    # check $a2 == "(value: 0, cnt: 2)"
    check split($(a2),'"')[1] == "(value: 0, cnt: 2, manual: false)"
    check not a2.isNil
    check a2[] == 0
    check split($(a3),'"')[1] == "(value: 0, cnt: 2, manual: false)"
    check not a3.isNil
    check a3[] == 0

  test "test destroy procs":
    runDestroyTest()

  test "test destroy release":
    runDestroyOnReleaseTest()

  test "test destroy release no proc":
    echo "\nintref setup:\n"
    let intref: ref int = new(ref int)
    intref[] = 30
    var a: SharedPtr[TestObj] = newSharedPtr(unsafeIsolate TestObj(val: intref))
    try:
      echo "a[]: ", a[]
      check a[].val[] == 30
    finally:
      a.release()
      check intref[] == 0

    ## important a should be nil now!
    ## to prevent future decr's from occuring
    check a.isNil == true
    a.decr()

  test "test destroy release generic no proc":
    echo "\nintref setup:\n"
    let intref: ref int = new(ref int)
    intref[] = 30
    var b: SharedPtr[TestObjGen[int]] = newSharedPtr(unsafeIsolate TestObjGen[int](val: intref))
    try:
      echo "a[]: ", b[]
      check b[].val[] == 30
    finally:
      b.release()
      check intref[] == 0
    
  test "test manual release / decr":
    echo "\nintref setup:\n"
    let intref: ref int = new(ref int)
    intref[] = 40
    var b = newSharedPtr(unsafeIsolate TestObjGen[int](val: intref), manualCount = 2)
    try:
      echo "a[]: ", b[]
      check b[].val[] == 40
    finally:
      b.decr()
      check intref[] == 40
      b.release()
      check intref[] == 0

  # TODO: add async test

