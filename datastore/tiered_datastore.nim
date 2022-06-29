import pkg/chronos
import pkg/questionable
import pkg/questionable/results
from pkg/stew/results as stewResults import get, isErr
import pkg/upraises

import ./datastore

export datastore

push: {.upraises: [].}

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

method contains*(
  self: TieredDatastore,
  key: Key): Future[?!bool] {.async, locks: "unknown".} =

  var
    exists = false

  for store in self.stores:
    let
      containsRes = await store.contains(key)

    if containsRes.isErr: return failure containsRes.error.msg

    exists = containsRes.get

    if exists: break

  return success exists

method delete*(
  self: TieredDatastore,
  key: Key): Future[?!void] {.async, locks: "unknown".} =

  var
    pending: seq[Future[?!void]]

  for store in self.stores:
    pending.add store.delete(key)

  await allFutures(pending)

  for fut in pending:
    let
      deleteRes = fut.read()

    if deleteRes.isErr: return failure deleteRes.error.msg

  return success()

method get*(
  self: TieredDatastore,
  key: Key): Future[?!(?seq[byte])] {.async, locks: "unknown".} =

  var
    bytesOpt: ?seq[byte]

  for store in self.stores:
    let
      getRes = await store.get(key)

    if getRes.isErr: return failure getRes.error.msg

    bytesOpt = getRes.get

    # put found data into stores logically in front of the current store
    if bytes =? bytesOpt:
      for s in self.stores:
        if s == store: break
        let
          putRes = await s.put(key, bytes)

        if putRes.isErr: return failure putRes.error.msg

      break

  return success bytesOpt

method put*(
  self: TieredDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async, locks: "unknown".} =

  var
    pending: seq[Future[?!void]]

  for store in self.stores:
    pending.add store.put(key, data)

  await allFutures(pending)

  for fut in pending:
    let
      putRes = fut.read()

    if putRes.isErr: return failure putRes.error.msg

  return success()

# method query*(
#   self: TieredDatastore,
#   query: ...): Future[?!(?...)] {.async, locks: "unknown".} =
#
#   return success ....none
