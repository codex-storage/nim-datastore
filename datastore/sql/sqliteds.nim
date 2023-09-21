import std/times
import std/options

import pkg/questionable
import pkg/questionable/results
import pkg/sqlite3_abi
from pkg/stew/results as stewResults import isErr
import pkg/upraises

import ../backend
import ./sqlitedsdb

export backend, sqlitedsdb

push: {.upraises: [].}

type
  SQLiteDatastore* = object
    db: SQLiteDsDb

proc path*(self: SQLiteDatastore): string =
  self.db.dbPath

proc readOnly*(self: SQLiteDatastore): bool = self.db.readOnly

proc timestamp*(t = epochTime()): int64 =
  (t * 1_000_000).int64

proc has*(self: SQLiteDatastore, key: DbKey): ?!bool =
  var
    exists = false

  proc onData(s: RawStmtPtr) =
    exists = sqlite3_column_int64(s, ContainsStmtExistsCol.cint).bool

  if err =? self.db.containsStmt.query((key.id), onData).errorOption:
    return failure err

  return success exists

proc delete*(self: SQLiteDatastore, key: DbKey): ?!void =
  return self.db.deleteStmt.exec((key.data))

proc delete*(self: SQLiteDatastore, keys: seq[DbKey]): ?!void =
  if err =? self.db.beginStmt.exec().errorOption:
    return failure(err)

  for key in keys:
    if err =? self.db.deleteStmt.exec((key.id)).errorOption:
      if err =? self.db.rollbackStmt.exec().errorOption:
        return failure err.msg

      return failure err.msg

  if err =? self.db.endStmt.exec().errorOption:
    return failure err.msg

  return success()

proc get*(self: SQLiteDatastore, key: DbKey): ?!seq[byte] =
  # see comment in ./filesystem_datastore re: finer control of memory
  # allocation in `proc get`, could apply here as well if bytes were read
  # incrementally with `sqlite3_blob_read`

  var
    bytes: seq[byte]

  proc onData(s: RawStmtPtr) =
    bytes = self.db.getDataCol()

  if err =? self.db.getStmt.query((key.id), onData).errorOption:
    return failure(err)

  if bytes.len <= 0:
    return failure(
      newException(DatastoreKeyNotFound, "DbKey doesn't exist"))

  return success bytes

proc put*(self: SQLiteDatastore, key: DbKey, data: seq[byte]): ?!void =
  return self.db.putStmt.exec((key.id, data, timestamp()))

proc put*(self: SQLiteDatastore, batch: seq[BatchEntry]): ?!void =
  if err =? self.db.beginStmt.exec().errorOption:
    return failure err

  for entry in batch:
    if err =? self.db.putStmt.exec((entry.key.id, entry.data, timestamp())).errorOption:
      if err =? self.db.rollbackStmt.exec().errorOption:
        return failure err

      return failure err

  if err =? self.db.endStmt.exec().errorOption:
    return failure err

  return success()

proc close*(self: SQLiteDatastore): ?!void =
  self.db.close()

  return success()


iterator query*(self: SQLiteDatastore,
              query: Query
              ): ?!ThreadQueryRes =

  var
    iter = QueryIter()
    queryStr = if query.value:
        QueryStmtDataIdStr
      else:
        QueryStmtIdStr

  if query.sort == SortOrder.Descending:
    queryStr &= QueryStmtOrderDescending
  else:
    queryStr &= QueryStmtOrderAscending

  if query.limit != 0:
    queryStr &= QueryStmtLimit

  if query.offset != 0:
    queryStr &=  QueryStmtOffset

  let
    queryStmt = QueryStmt.prepare(
      self.db.env, queryStr).expect("should not fail")

    s = RawStmtPtr(queryStmt)

  var
    v = sqlite3_bind_text(
      s, 1.cint, (query.key.id & "*").cstring, -1.cint, SQLITE_TRANSIENT_GCSAFE)

  if not (v == SQLITE_OK):
    return failure newException(DatastoreError, $sqlite3_errstr(v))

  if query.limit != 0:
    v = sqlite3_bind_int(s, 2.cint, query.limit.cint)

    if not (v == SQLITE_OK):
      return failure newException(DatastoreError, $sqlite3_errstr(v))

  if query.offset != 0:
    v = sqlite3_bind_int(s, 3.cint, query.offset.cint)

    if not (v == SQLITE_OK):
      return failure newException(DatastoreError, $sqlite3_errstr(v))

  let lock = newAsyncLock()
  proc next(): ?!QueryResponse =
    defer:
      if lock.locked:
        lock.release()

    if lock.locked:
      return failure (ref DatastoreError)(msg: "Should always await query features")

    if iter.finished:
      return failure((ref QueryEndedError)(msg: "Calling next on a finished query!"))

    await lock.acquire()

    let
      v = sqlite3_step(s)

    case v
    of SQLITE_ROW:
      let
        key = DbKey.init(
          $sqlite3_column_text_not_null(s, QueryStmtIdCol))
          .expect("should not fail")

        blob: ?pointer =
          if query.value:
              sqlite3_column_blob(s, QueryStmtDataCol).some
            else:
              pointer.none

      # detect out-of-memory error
      # see the conversion table and final paragraph of:
      # https://www.sqlite.org/c3ref/column_blob.html
      # see also https://www.sqlite.org/rescode.html

      # the "data" column can be NULL so in order to detect an out-of-memory
      # error it is necessary to check that the result is a null pointer and
      # that the result code is an error code
      if blob.isSome and blob.get().isNil:
        let
          v = sqlite3_errcode(sqlite3_db_handle(s))

        if not (v in [SQLITE_OK, SQLITE_ROW, SQLITE_DONE]):
          iter.finished = true
          return failure newException(DatastoreError, $sqlite3_errstr(v))

      let
        dataLen = sqlite3_column_bytes(s, QueryStmtDataCol)
        data = if blob.isSome:
            @(
              toOpenArray(cast[ptr UncheckedArray[byte]](blob.get),
              0,
              dataLen - 1))
          else:
            @[]

      return success (key.some, data)
    of SQLITE_DONE:
      iter.finished = true
      return success (DbKey.none, EmptyBytes)
    else:
      iter.finished = true
      return failure newException(DatastoreError, $sqlite3_errstr(v))

  iter.dispose = proc(): ?!void =
    discard sqlite3_reset(s)
    discard sqlite3_clear_bindings(s)
    s.dispose
    return success()

  iter.next = next
  return success iter

proc new*(T: type SQLiteDatastore,
          path: string,
          readOnly = false): ?!T =

  let
    flags =
      if readOnly: SQLITE_OPEN_READONLY
      else: SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE

  success SQLiteDatastore(db: ? SQLiteDsDb.open(path, flags))
    

proc new*(T: type SQLiteDatastore,
          db: SQLiteDsDb): ?!T =

  success SQLiteDatastore(db: db)
