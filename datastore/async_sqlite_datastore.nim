import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/upraises

import ./datastore
import ./sqlite
import ./sqlite_datastore

export chronos, datastore, sqlite
export dataCol, dbExt, dbPath, env, idCol, tableTitle, timestamp, timestampCol

push: {.upraises: [].}

type
  AsyncSQLiteDatastore* = ref object of Datastore
    store: SQLiteDatastore

proc new*(
  T: type AsyncSQLiteDatastore,
  basePath = "data",
  filename = "store" & dbExt,
  readOnly = false,
  inMemory = false): ?!T =

  let
    store = ? SQLiteDatastore.new(basePath, filename, readOnly, inMemory)

  success T(store: store)

proc store*(self: AsyncSQLiteDatastore): SQLiteDatastore =
  self.store

proc close*(self: AsyncSQLiteDatastore) =
  if not self.store.isNil: self.store.close
  self[] = AsyncSQLiteDatastore()[]

method contains*(
  self: AsyncSQLiteDatastore,
  key: Key): Future[?!bool] {.async, base, locks: "unknown".} =

  return self.store.contains(key)

method delete*(
  self: AsyncSQLiteDatastore,
  key: Key): Future[?!void] {.async, base, locks: "unknown".} =

  return self.store.delete(key)

method get*(
  self: AsyncSQLiteDatastore,
  key: Key): Future[?!(?seq[byte])] {.async, base, locks: "unknown".} =

  return self.store.get(key)

method put*(
  self: AsyncSQLiteDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async, base, locks: "unknown".} =

  return self.store.put(key, data)

# method query*(
#   self: AsyncSQLiteDatastore,
#   query: ...): Future[?!(?...)] {.async, base, locks: "unknown".} =
#
#   return self.query(query)
