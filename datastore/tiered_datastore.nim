import pkg/questionable
import pkg/questionable/results
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
  key: Key): ?!bool {.locks: "unknown".} =

  var
    exists = false

  for store in self.stores:
    exists = ? store.contains(key)
    if exists: break

  success exists

method delete*(
  self: TieredDatastore,
  key: Key): ?!void {.locks: "unknown".} =

  for store in self.stores:
    ? store.delete(key)

  success()

method get*(
  self: TieredDatastore,
  key: Key): ?!(?seq[byte]) {.locks: "unknown".} =

  var
    bytesOpt: ?seq[byte]

  for store in self.stores:
    bytesOpt = ? store.get(key)

    # put found data into stores logically in front of the current store
    if bytes =? bytesOpt:
      for s in self.stores:
        if s == store: break
        ? s.put(key, bytes)
      break

  success bytesOpt

method put*(
  self: TieredDatastore,
  key: Key,
  data: openArray[byte]): ?!void {.locks: "unknown".} =

  for store in self.stores:
    ? store.put(key, data)

  success()

# method query*(
#   self: TieredDatastore,
#   query: ...): ?!(?...) {.locks: "unknown".} =
#
#   success ....none
