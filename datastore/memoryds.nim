import std/tables

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

method get*(
    self: MemoryDatastore,
    key: Key
): Future[?!seq[byte]] {.async.} =

  let dk = KeyBuffer.new(key)
  if self.store.hasKey(dk):
    let res = self.store[dk]
    return success res.toSeq(byte)
  else:
    return failure (ref DatastoreError)(msg: "no such key")

method put*(
    self: MemoryDatastore,
    key: Key,
    data: seq[byte]
): Future[?!void] {.async.} =

  let dk = KeyBuffer.new(key)
  let dv = ValueBuffer.new(key)
  self.store[dk] = dv
  return success()

method put*(
  self: MemoryDatastore,
  batch: seq[BatchEntry]): Future[?!void] {.async.} =

  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err

  return success()

method close*(self: MemoryDatastore): Future[?!void] {.async.} =
  self.store.clear()
  return success()

func new*(tp: typedesc[MemoryDatastore]): ?!MemoryDatastore =
  var self = default(tp)
  success self
