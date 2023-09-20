import std/tables
import std/sequtils
import std/strutils
import std/algorithm

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/upraises

import ./key
import ./query
import ./datastore
import ./threads/databuffer

export key, query

push: {.upraises: [].}

import std/locks

type
  SimpleTable*[N: static int] = object
    data*: array[N, tuple[used: bool, key: KeyBuffer, val: ValueBuffer]]

proc hasKey*[N](table: var SimpleTable[N], key: KeyBuffer): bool =
  for (u, k, _) in table.data:
    if u and key == k:
      return true

proc `[]`*[N](table: var SimpleTable[N], key: KeyBuffer): ValueBuffer {.raises: [KeyError].} =
  for item in table.data:
    if item.used and item.key == key:
      return item.val
  raise newException(KeyError, "no such key")

proc `[]=`*[N](table: var SimpleTable[N], key: KeyBuffer, value: ValueBuffer) =
  for item in table.data.mitems():
    if item.key == key:
      item = (true, key, value)
      return
  # key not found, find free item
  for item in table.data.mitems():
    if item.used == false:
      item = (true, key, value)
      return

proc clear*[N](table: var SimpleTable[N]) =
  for item in table.data.mitems():
    item.used = false

proc pop*[N](table: var SimpleTable[N], key: KeyBuffer, value: var ValueBuffer): bool =
  for item in table.data.mitems():
    if item.used and item.key == key:
      value = item.val
      item.used = false
      return true

iterator keys*[N](table: var SimpleTable[N]): KeyBuffer =
  for (u, k, _) in table.data:
    if u:
      yield k

when isMainModule:
  import unittest2

  suite "simple table":

    var table: SimpleTable[10]
    let k1 = KeyBuffer.new("k1")
    let k2 = KeyBuffer.new("k2")
    let v1 = ValueBuffer.new("hello world!")
    let v2 = ValueBuffer.new("other val")

    test "put":
      table[k1] = v1
      table[k2] = v2
    test "hasKey":
      check table.hasKey(k1)
      check table.hasKey(k2)
    test "get":
      check table[k1].toString == "hello world!"
      check table[k2].toString == "other val"
    test "delete":
      var res: ValueBuffer
      check table.pop(k1, res)
      check res.toString == "hello world!"
      expect KeyError:
        let res = table[k1]
        check res.toString == "hello world!"
    test "put new":
      table[k1] = v1
      table[k1] = v2
      let res = table[k1]
      check res.toString == "other val"

type
  MemoryDatastore* = ref object of Datastore
    lock*: Lock
    store*: SimpleTable[10_000]

method has*(
    self: MemoryDatastore,
    key: Key
): Future[?!bool] {.async.} =

  let dk = KeyBuffer.new(key)
  withLock(self.lock):
    return success self.store.hasKey(dk)

method delete*(
    self: MemoryDatastore,
    key: Key
): Future[?!void] {.async.} =

  let dk = KeyBuffer.new(key)
  var val: ValueBuffer
  withLock(self.lock):
    discard self.store.pop(dk, val)
  return success()

method delete*(
  self: MemoryDatastore,
  keys: seq[Key]): Future[?!void] {.async.} =

  for key in keys:
    if err =? (await self.delete(key)).errorOption:
      return failure err

  return success()

method get*(
    self: MemoryDatastore,
    key: Key
): Future[?!seq[byte]] {.async.} =

  let dk = KeyBuffer.new(key)
  withLock(self.lock):
    if self.store.hasKey(dk):
      let res = self.store[dk].toSeq(byte)
      return success res
    else:
      return failure (ref DatastoreError)(msg: "no such key")

method put*(
    self: MemoryDatastore,
    key: Key,
    data: seq[byte]
): Future[?!void] {.async.} =

  let dk = KeyBuffer.new(key)
  let dv = ValueBuffer.new(data)
  withLock(self.lock):
    self.store[dk] = dv
  return success()

method put*(
  self: MemoryDatastore,
  batch: seq[BatchEntry]): Future[?!void] {.async.} =

  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err

  return success()

proc keyIterator(self: MemoryDatastore, queryKey: string): iterator: KeyBuffer {.gcsafe.} =
  return iterator(): KeyBuffer {.closure.} =
    var keys = self.store.keys().toSeq()
    keys.sort(proc (x, y: KeyBuffer): int = cmp(x.toString, y.toString))
    for key in keys:
      if key.toString().startsWith(queryKey):
        yield key 

method query*(
  self: MemoryDatastore,
  query: Query,
): Future[?!QueryIter] {.async.} =

  let
    queryKey = query.key.id()
    walker = keyIterator(self, queryKey)
  var
    iter = QueryIter.new()
  iter.readyForNext = true

  proc next(): Future[?!QueryResponse] {.async.} =
    iter.readyForNext = false
    let kb = walker()
    iter.readyForNext = true

    if finished(walker):
      iter.finished = true
      return success (Key.none, EmptyBytes)

    let key = kb.toKey()
    var ds: ValueBuffer
    withLock(self.lock):
      if query.value:
        ds = self.store[kb]
    let data = if ds.isNil: EmptyBytes else: ds.toSeq(byte)

    return success (key.some, data)

  iter.next = next
  return success iter

method close*(self: MemoryDatastore): Future[?!void] {.async.} =
  self.store.clear()
  return success()

func new*(tp: typedesc[MemoryDatastore]): MemoryDatastore =
  var self = tp()
  self.lock.initLock()
  return self
