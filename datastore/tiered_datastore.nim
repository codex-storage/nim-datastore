import std/sequtils

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
from pkg/stew/results as stewResults import isErr
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

  for store in self.stores:
    without contain =? await store.contains(key), error:
      return failure error

    if contain: return success true

  return success false

method delete*(
  self: TieredDatastore,
  key: Key): Future[?!void] {.async, locks: "unknown".} =

  let
    pending = await allFinished(self.stores.mapIt(it.delete(key)))

  for fut in pending:
    if fut.read().isErr: return fut.read()

  return success()

method get*(
  self: TieredDatastore,
  key: Key): Future[?!(?seq[byte])] {.async, locks: "unknown".} =

  var
    bytesOpt: ?seq[byte]

  for store in self.stores:
    # the following commented line does not produce the expected result at runtime:
    # without bytesOpt =? await store.get(key), error:

    # instead we use intermediate identifier `bopt`
    without bOpt =? await store.get(key), error:
      return failure error

    # and now assign `bopt` value to `bytesOpt`
    bytesOpt = bOpt

    # put found data into stores logically in front of the current store
    if bytes =? bytesOpt:
      for s in self.stores:
        if s == store: break
        let
          putRes = await s.put(key, bytes)

        if putRes.isErr: return failure putRes.error

      break

  return success bytesOpt

method put*(
  self: TieredDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async, locks: "unknown".} =

  let
    pending = await allFinished(self.stores.mapIt(it.put(key, data)))

  for fut in pending:
    if fut.read().isErr: return fut.read()

  return success()

# method query*(
#   self: TieredDatastore,
#   query: ...): Future[?!(?...)] {.async, locks: "unknown".} =
#
#   return success ....some
