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
  MountedStore* = object
    store*: Datastore
    key*: Key

  MountedDatastore* = ref object of Datastore
    stores*: Table[Key, MountedStore]

method mount*(self: MountedDatastore, key: Key, store: Datastore): ?!void =
  ## Mount a store on a namespace - namespaces are only `/`
  ##

  if key in self.stores:
    return failure("Key already has store mounted!")

  self.stores.add(key, MountedStore(store: store, key: key))

  return success()

func findStore*(self: MountedDatastore, key: Key): ?!MountedStore =
  ## Find a store mounted under a particular key
  ##

  for (k, v) in self.stores.pairs:
    var
      mounted = key

    while mounted.len > 0:
      if k == mounted:
        return success v

      if mounted.parent.isErr:
        break

      mounted = mounted.parent.get

  failure newException(DatastoreKeyNotFound, "No datastore found for key")

proc dispatch(
  self: MountedDatastore,
  key: Key): ?!tuple[store: MountedStore, relative: Key] =
  ## Helper to retrieve the store and corresponding relative key
  ##

  let
    mounted = ?self.findStore(key)

  return success (store: mounted, relative: ?key.relative(mounted.key))

method contains*(
  self: MountedDatastore,
  key: Key): Future[?!bool] {.async.} =

  without mounted =? self.dispatch(key):
    return failure "No mounted datastore found"

  return (await mounted.store.store.contains(mounted.relative))

method delete*(
  self: MountedDatastore,
  key: Key): Future[?!void] {.async.} =

  without mounted =? self.dispatch(key), error:
    return failure(error)

  return (await mounted.store.store.delete(mounted.relative))

method get*(
  self: MountedDatastore,
  key: Key): Future[?!seq[byte]] {.async.} =

  without mounted =? self.dispatch(key), error:
    return failure(error)

  return await mounted.store.store.get(mounted.relative)

method put*(
  self: MountedDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async.} =

  without mounted =? self.dispatch(key), error:
    return failure(error)

  return (await mounted.store.store.put(mounted.relative, data))

func new*(
  T: type MountedDatastore,
  stores: Table[Key, Datastore] = initTable[Key, Datastore]()): ?!T =

  var self = T()
  for (k, v) in stores.pairs:
    self.stores.add(k, MountedStore(store: v, key: k))

  success self
