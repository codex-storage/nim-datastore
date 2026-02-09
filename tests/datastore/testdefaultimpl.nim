import std/options
import std/sequtils
import std/tables

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import pkg/datastore
import pkg/datastore/defaultimpl

type
  MockDatastore* = ref object of Datastore
    values: TableRef[Key, seq[byte]]
    lock: AsyncLock
    calls: seq[MethodCall]

  Method = enum
    Put, Get, Delete, Has
  
  MethodCall = object
    key: Key
    case kind: Method
    of Put:
      data: seq[byte]
    of Get, Delete, Has:
      discard

method put*(self: MockDatastore, key: Key, data: seq[byte]): Future[?!void] {.async.} =
  self.calls.add(MethodCall(kind: Put, key: key, data: data))
  self.values[key] = data
  success()

method get*(self: MockDatastore, key: Key): Future[?!seq[byte]] {.async.} =
  self.calls.add(MethodCall(kind: Get, key: key))
  if key notin self.values:
    failure(newException(DatastoreKeyNotFound, "Key doesn't exist"))
  else:
    success(self.values[key])

method has*(self: MockDatastore, key: Key): Future[?!bool] {.async.} =
  self.calls.add(MethodCall(kind: Has, key: key))
  success(key in self.values)

method delete*(self: MockDatastore, key: Key): Future[?!void] {.async.} =
  self.calls.add(MethodCall(kind: Delete, key: key))
  self.values.del(key)
  success()

method modifyGet*(self: MockDatastore, key: Key, fn: ModifyGet): Future[?!seq[byte]] {.async.} =
  await defaultModifyGetImpl(self, self.lock, key, fn)

method modify*(self: MockDatastore, key: Key, fn: Modify): Future[?!void] {.async.} =
  await defaultModifyImpl(self, self.lock, key, fn)

proc new*(
  T: type MockDatastore): T =
  T(
    values: newTable[Key, seq[byte]](),
    lock: newAsyncLock(),
    calls: newSeq[MethodCall]()
    )

suite "Test defaultimpl":
  var
    ds: MockDatastore
    key: Key

  setup:
    ds = MockDatastore.new()
    key = Key.init("/a/b").tryGet()

  test "should put a value that is different than the current value":

    (await ds.put(key, @[byte 1, 2, 3])).tryGet()

    proc modifyFn(maybeCurr: ?seq[byte]): Future[?seq[byte]] {.async.} =
      some(@[byte 3, 2, 1])

    (await ds.modify(key, modifyFn)).tryGet()

    check:
      ds.calls.filterIt(it.kind == Put).len == 2

  test "should not attempt to put a value that is equal to the current value":
    (await ds.put(key, @[byte 1, 2, 3])).tryGet()

    proc modifyFn(maybeCurr: ?seq[byte]): Future[?seq[byte]] {.async.} =
      some(@[byte 1, 2, 3])

    (await ds.modify(key, modifyFn)).tryGet()

    check:
      ds.calls.filterIt(it.kind == Put).len == 1
