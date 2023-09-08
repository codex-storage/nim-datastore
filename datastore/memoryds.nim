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

export key, query

push: {.upraises: [].}

type
  MemoryDatastore* = ref object of Datastore
    store*: Table[Key, seq[byte]]

method has*(
    self: MemoryDatastore,
    key: Key
): Future[?!bool] {.async.} =

  return success self.store.hasKey(key)

method delete*(
  self: MemoryDatastore,
  key: Key): Future[?!void] {.async.} =

  self.store.del(key)
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
  key: Key): Future[?!seq[byte]] {.async.} =

  self.store.withValue(key, value):
    return success value[]
  do:
    return failure (ref DatastoreError)(msg: "no such key")

method put*(
  self: MemoryDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async.} =

  self.store[key] = data
  return success()

method put*(
  self: MemoryDatastore,
  batch: seq[BatchEntry]): Future[?!void] {.async.} =

  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err

  return success()

# proc keyIterator(self: MemoryDatastore, queryKey: string): iterator: KeyBuffer {.gcsafe.} =
#   return iterator(): KeyBuffer {.closure.} =
#     var keys = self.store.keys().toSeq()
#     keys.sort(proc (x, y: KeyBuffer): int = cmp(x.toString, y.toString))
#     for key in keys:
#       if key.toString().startsWith(queryKey):
#         yield key

# method query*(
#   self: MemoryDatastore,
#   query: Query,
# ): Future[?!QueryIter] {.async.} =

#   let
#     queryKey = query.key.id()
#     walker = keyIterator(self, queryKey)
#   var
#     iter = QueryIter.new()

#   proc next(): Future[?!QueryResponse] {.async.} =
#     let kb = walker()

#     if finished(walker):
#       iter.finished = true
#       return success (Key.none, EmptyBytes)

#     let key = kb.toKey().expect("should not fail")
#     var ds: ValueBuffer
#     if query.value:
#       ds = self.store[kb]
#     let data = if ds.isNil: EmptyBytes else: ds.toSeq(byte)

#     return success (key.some, data)

#   iter.next = next
#   return success iter

method close*(self: MemoryDatastore): Future[?!void] {.async.} =
  self.store.clear()
  return success()

func new*(tp: type MemoryDatastore): MemoryDatastore =
  var self = tp()
  return self
