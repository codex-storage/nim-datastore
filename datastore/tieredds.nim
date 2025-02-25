{.push raises: [].}

import std/sequtils

import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./datastore

export datastore

type
  TieredDatastore* = ref object of Datastore
    stores: seq[Datastore]

proc new*(
  T: type TieredDatastore,
  stores: varargs[Datastore]): ?!T =

  if stores.len == 0:
    failure "stores must contain at least one Datastore"
  else:
    success T(stores: @stores)

proc stores*(self: TieredDatastore): seq[Datastore] =
  self.stores

method has*(
  self: TieredDatastore,
  key: Key): Future[?!bool] {.async: (raises: [CancelledError]).} =

  for store in self.stores:
    without res =? (await store.has(key)), err:
      return failure(err)

    if res:
      return success true

  return success false

method delete*(
  self: TieredDatastore,
  key: Key): Future[?!void] {.async: (raises: [CancelledError]).} =

  let
    pending = await allFinished(self.stores.mapIt(it.delete(key)))

  for fut in pending:
    try:
      if fut.read().isErr:
        return fut.read()
    except FuturePendingError as err:
      return failure err

  return success()

method delete*(
  self: TieredDatastore,
  keys: seq[Key]): Future[?!void] {.async: (raises: [CancelledError]).} =

  for key in keys:
    let
      pending = await allFinished(self.stores.mapIt(it.delete(key)))

    for fut in pending:
      try:
        if fut.read().isErr:
          return fut.read()
      except FuturePendingError as err:
        return failure err

  return success()

method get*(
  self: TieredDatastore,
  key: Key): Future[?!seq[byte]] {.async: (raises: [CancelledError]).} =

  var
    bytes: seq[byte]

  for store in self.stores:
    without bytes =? (await store.get(key)):
      continue

    if bytes.len <= 0:
      continue

    # put found data into stores logically in front of the current store
    for s in self.stores:
      if s == store: break
      if(
        let res = (await s.put(key, bytes));
        res.isErr):
        return failure res.error

    return success bytes

method put*(
  self: TieredDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async: (raises: [CancelledError]).} =

  let
    pending = await allFinished(self.stores.mapIt(it.put(key, data)))

  for fut in pending:
    try:
      if fut.read().isErr:
        return fut.read()
    except FuturePendingError as err:
      return failure err

  return success()

method put*(
  self: TieredDatastore,
  batch: seq[BatchEntry]): Future[?!void] {.async: (raises: [CancelledError]).} =

  for entry in batch:
    let
      pending = await allFinished(self.stores.mapIt(it.put(entry.key, entry.data)))

    for fut in pending:
      try:
        if fut.read().isErr:
          return fut.read()
      except FuturePendingError as err:
        return failure err

  return success()

method modifyGet*(
  self: TieredDatastore,
  key: Key,
  fn: ModifyGet): Future[?!seq[byte]] {.async: (raises: [CancelledError]).} =

  let
    pending = await allFinished(self.stores.mapIt(it.modifyGet(key, fn)))

  var aux = newSeq[byte]()

  for fut in pending:
    try:
      if fut.read().isErr:
        return fut.read()
      else:
        aux.add(fut.read().get)
    except FuturePendingError as err:
      return failure err

  return success(aux)

method modify*(
  self: TieredDatastore,
  key: Key,
  fn: Modify): Future[?!void] {.async: (raises: [CancelledError]).} =

  let
    pending = await allFinished(self.stores.mapIt(it.modify(key, fn)))

  for fut in pending:
    try:
      if fut.read().isErr:
        return fut.read()
    except FuturePendingError as err:
      return failure err

  return success()

# method query*(
#   self: TieredDatastore,
#   query: ...): Future[?!(?...)] {.async.} =
#
#   return success ....some
