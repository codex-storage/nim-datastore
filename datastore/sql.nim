import std/times
import std/options

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/sqlite3_abi
from pkg/stew/results as stewResults import isErr
import pkg/upraises

import std/sequtils
import ../datastore
import ./backend
import ./sql/sqliteds

export datastore

push: {.upraises: [].}

type
  SQLiteDatastore* = ref object of Datastore
    db: SQLiteBackend[KeyId, DataBuffer]

proc path*(self: SQLiteDatastore): string =
  self.db.path()

proc readOnly*(self: SQLiteDatastore): bool =
  self.db.readOnly()

method has*(self: SQLiteDatastore,
            key: Key): Future[?!bool] {.async.} =
  return self.db.has(KeyId.new key.id())

method delete*(self: SQLiteDatastore,
               key: Key): Future[?!void] {.async.} =
  return self.db.delete(KeyId.new key.id())

method delete*(self: SQLiteDatastore,
               keys: seq[Key]): Future[?!void] {.async.} =
  let dkeys = keys.mapIt(KeyId.new it.id())
  return self.db.delete(dkeys)

method get*(self: SQLiteDatastore,
            key: Key): Future[?!seq[byte]] {.async.} =
  self.db.get(KeyId.new key.id())

method put*(self: SQLiteDatastore,
            key: Key,
            data: seq[byte]): Future[?!void] {.async.} =
  self.db.put(KeyId.new key.id(), DataBuffer.new data)

method put*(self: SQLiteDatastore,
            batch: seq[BatchEntry]): Future[?!void] {.async.} =
  var dbatch: seq[tuple[key: KeyId, data: DataBuffer]]
  for entry in batch:
    dbatch.add((KeyId.new entry.key.id(), DataBuffer.new entry.data))
  self.db.put(dbatch)

method close*(self: SQLiteDatastore): Future[?!void] {.async.} =
  self.db.close()

method query*(
  self: SQLiteDatastore,
  query: Query
): ?!(iterator(): ?!QueryResponse) =

  let dbquery = dbQuery(
    key= KeyId.new query.key.id(),
    value= query.value,
    limit= query.limit,
    offset= query.offset,
    sort= query.sort,
  )
  var qhandle = ? self.db.query(dbquery)

  let iter = iterator(): ?!QueryResponse =
    for resp in qhandle.iter():
      without qres =? resp, err:
        yield QueryResponse.failure err
  
  success iter

proc new*(
  T: type SQLiteDatastore,
  path: string,
  readOnly = false): ?!T =

  let
    flags =
      if readOnly: SQLITE_OPEN_READONLY
      else: SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE

  success T(
    db: ? SQLiteDsDb.open(path, flags),
    readOnly: readOnly)

proc new*(
  T: type SQLiteDatastore,
  db: SQLiteDsDb): ?!T =

  success T(
    db: db,
    readOnly: db.readOnly)
