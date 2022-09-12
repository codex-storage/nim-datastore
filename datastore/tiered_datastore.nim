import std/sequtils

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

method close*(self: TieredDatastore) {.async, locks: "unknown".} =
  for store in self.stores:
    await store.close

method contains*(
  self: TieredDatastore,
  key: Key): Future[?!bool] {.async, locks: "unknown".} =

  for store in self.stores:
    let
      containsRes = await store.contains(key)

    if containsRes.isErr: return containsRes
    if containsRes.get == true: return success true

  return success false

method delete*(
  self: TieredDatastore,
  key: Key): Future[?!void] {.async, locks: "unknown".} =

  let
    pending = await allFinished(self.stores.mapIt(it.delete(key)))

  for fut in pending:
    let
      delRes = await fut

    if delRes.isErr: return delRes

  return success()

method get*(
  self: TieredDatastore,
  key: Key): Future[?!(?seq[byte])] {.async, locks: "unknown".} =

  var
    bytesOpt: ?seq[byte]

  for store in self.stores:
    let
      getRes = await store.get(key)

    if getRes.isErr: return getRes

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

  let
    pending = await allFinished(self.stores.mapIt(it.put(key, data)))

  for fut in pending:
    let
      putRes = await fut

    if putRes.isErr: return putRes

  return success()

iterator queryImpl(
  datastore: Datastore,
  query: Query): Future[QueryResponse] {.closure.} =

  let
    datastore = TieredDatastore(datastore)
    # https://github.com/datastore/datastore/blob/7ccf0cd4748001d3dbf5e6dda369b0f63e0269d3/datastore/core/basic.py#L1027-L1035
    bottom = datastore.stores[^1]

  try:
    let q = bottom.query(); for kv in q(bottom, query): yield kv
  except Exception as e:
    raise (ref Defect)(msg: e.msg)

method query*(self: TieredDatastore): QueryIterator {.locks: "unknown".} =
  queryImpl
