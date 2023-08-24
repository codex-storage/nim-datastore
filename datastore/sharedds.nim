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

  SharedDatastore* = ref object of Datastore
    # stores*: Table[Key, SharedDatastore]

method has*(
  self: SharedDatastore,
  key: Key
): Future[?!bool] {.async.} =

  # without mounted =? self.dispatch(key):
  #   return failure "No mounted datastore found"
  # return (await mounted.store.store.has(mounted.relative))
  return success(true)

method delete*(
  self: SharedDatastore,
  key: Key
): Future[?!void] {.async.} =

  # without mounted =? self.dispatch(key), error:
  #   return failure(error)
  # return (await mounted.store.store.delete(mounted.relative))
  return success()

method delete*(
  self: SharedDatastore,
  keys: seq[Key]
): Future[?!void] {.async.} =

  # for key in keys:
  #   if err =? (await self.delete(key)).errorOption:
  #     return failure err

  return success()

method get*(
  self: SharedDatastore,
  key: Key
): Future[?!seq[byte]] {.async.} =

  # without mounted =? self.dispatch(key), error:
  #   return failure(error)

  # return await mounted.store.store.get(mounted.relative)
  return success(newSeq[byte]())

method put*(
  self: SharedDatastore,
  key: Key,
  data: seq[byte]
): Future[?!void] {.async.} =

  # without mounted =? self.dispatch(key), error:
  #   return failure(error)

  # return (await mounted.store.store.put(mounted.relative, data))
  return success()

method put*(
  self: SharedDatastore,
  batch: seq[BatchEntry]
): Future[?!void] {.async.} =

  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err

  return success()

method close*(
  self: SharedDatastore
): Future[?!void] {.async.} =

  # for s in self.stores.values:
  #   discard await s.store.close()

  # TODO: how to handle failed close?
  return success()

func new*[S: ref Datastore](
  T: typedesc[SharedDatastore],
  storeTp: typedesc[S]
): ?!SharedDatastore =

  var self = T()
  # for (k, v) in stores.pairs:
  #   self.stores[?k.path] = MountedStore(store: v, key: k)

  success self
