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
import ./databuffer

export key, query

push: {.upraises: [].}

type

  MemoryDatastore* = ref object of Datastore
    store*: Table[KeyBuffer, ValueBuffer]

method has*(
    self: MemoryDatastore,
    key: Key
): Future[?!bool] {.async.} =

  let dk = KeyBuffer.new(key)
  return success self.store.hasKey(dk)

method delete*(
    self: MemoryDatastore,
    key: Key
): Future[?!void] {.async.} =

  let dk = KeyBuffer.new(key)
  var val: ValueBuffer
  discard self.store.pop(dk, val)
  return success()

method delete*(
  self: MemoryDatastore,
  keys: seq[Key]): Future[?!void] {.async.} =

  for key in keys:
    if err =? (await self.delete(key)).errorOption:
      return failure err

  return success()

import pretty

method get*(
    self: MemoryDatastore,
    key: Key
): Future[?!seq[byte]] {.async.} =

  let dk = KeyBuffer.new(key)
  echo "getting: ", key
  for k, v in self.store.pairs():
    print "get: ", k.toString(), " v: ", v.toString().repr

  if self.store.hasKey(dk):
    let res = self.store[dk].toSeq(byte)
    print "get:res: ", res
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

import pretty

method query*(
  self: MemoryDatastore,
  query: Query,
): Future[?!QueryIter] {.async.} =

  let
    queryKey = query.key.id()
    walker = keyIterator(self, queryKey)
  var
    iter = QueryIter.new()

  proc next(): Future[?!QueryResponse] {.async.} =
    let kb = walker()

    if finished(walker):
      iter.finished = true
      return success (Key.none, EmptyBytes)

    let key = kb.toKey().expect("should not fail")
    var ds: ValueBuffer
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
  return self
