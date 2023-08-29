import std/tables

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
  MemoryStore* = object
    store*: Datastore
    key*: Key

  MemoryDatastore* = ref object of Datastore
    stores*: Table[Key, MemoryStore]

method has*(
  self: MemoryDatastore,
  key: Key): Future[?!bool] {.async.} =

  without mounted =? self.dispatch(key):
    return failure "No mounted datastore found"

  return (await mounted.store.store.has(mounted.relative))

method delete*(
  self: MemoryDatastore,
  key: Key): Future[?!void] {.async.} =

  without mounted =? self.dispatch(key), error:
    return failure(error)

  return (await mounted.store.store.delete(mounted.relative))

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

  without mounted =? self.dispatch(key), error:
    return failure(error)

  return await mounted.store.store.get(mounted.relative)

method put*(
  self: MemoryDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async.} =

  without mounted =? self.dispatch(key), error:
    return failure(error)

  return (await mounted.store.store.put(mounted.relative, data))

method put*(
  self: MemoryDatastore,
  batch: seq[BatchEntry]): Future[?!void] {.async.} =

  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err

  return success()

method close*(self: MemoryDatastore): Future[?!void] {.async.} =
  for s in self.stores.values:
    discard await s.store.close()

  # TODO: how to handle failed close?
  return success()

func new*(
  T: type MemoryDatastore,
  stores: Table[Key, Datastore] = initTable[Key, Datastore]()): ?!T =

  var self = T()
  for (k, v) in stores.pairs:
    self.stores[?k.path] = MemoryStore(store: v, key: k)

  success self
