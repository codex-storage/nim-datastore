import std/tables

import pkg/chronos
import pkg/chronos/threadsync
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

  let signal = ThreadSignalPtr.new().valueOr:
    return failure newException(DatastoreError, "error creating signal")

  await wait(signal)
  return success()

method put*(
  self: SharedDatastore,
  batch: seq[BatchEntry]
): Future[?!void] {.async.} =
  raiseAssert("Not implemented!")

method close*(
  self: SharedDatastore
): Future[?!void] {.async.} =

  # TODO: how to handle failed close?
  return success()

func new*[S: ref Datastore](
  T: typedesc[SharedDatastore],
  storeTp: typedesc[S]
): ?!SharedDatastore =

  var self = SharedDatastore()

  success self
