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

type

  MemoryDatastore* = ref object of Datastore
    store*: Table[DataBuffer, DataBuffer]

method has*(
    self: MemoryDatastore,
    key: Key
): Future[?!bool] {.async.} =

  let dk = DataBuffer.new(key.id())
  return success self.store.hasKey(dk)

method delete*(
    self: MemoryDatastore,
    key: Key
): Future[?!void] {.async.} =

  let dk = DataBuffer.new(key.id())
  var val: DataBuffer
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

  let dk = DataBuffer.new(key.id())
  if self.store.hasKey(dk):
    let res = self.store[dk].toSeq()
    return success res
  else:
    return failure (ref DatastoreError)(msg: "no such key")

method put*(
    self: MemoryDatastore,
    key: Key,
    data: seq[byte]
): Future[?!void] {.async.} =

  let dk = DataBuffer.new(key.id())
  let dv = DataBuffer.new(data)
  self.store[dk] = dv
  return success()

method put*(
  self: MemoryDatastore,
  batch: seq[BatchEntry]): Future[?!void] {.async.} =

  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err

  return success()

proc keyIterator(self: MemoryDatastore, queryKey: string): iterator: DataBuffer {.gcsafe.} =
  return iterator(): DataBuffer {.closure.} =
    var keys = self.store.keys().toSeq()
    keys.sort(proc (x, y: DataBuffer): int = cmp(x.toString, y.toString))
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
  # iter.readyForNext = true

  proc next(): Future[?!QueryResponse] {.async.} =
    # iter.readyForNext = false
    let kb = walker()
    # iter.readyForNext = true

    if finished(walker):
      iter.finished = true
      return success (Key.none, EmptyBytes)

    let key = Key.init(kb.toString()).get()
    var ds: DataBuffer
    if query.value:
      ds = self.store[kb]
    let data = if ds.isNil: EmptyBytes else: ds.toSeq()

    let res: tuple[key: ?Key, data: seq[byte]] = (key.some, data)
    return success(res)

  iter.next = next
  return success iter

method close*(self: MemoryDatastore): Future[?!void] {.async.} =
  self.store.clear()
  return success()

func new*(tp: typedesc[MemoryDatastore]): MemoryDatastore =
  var self = tp()
  return self
