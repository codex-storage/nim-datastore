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
    key = $key

  proc onData(s: RawStmtPtr) =
    exists = sqlite3_column_int64(s, ContainsStmtExistsCol.cint).bool

  if err =? self.db.containsStmt.query((key), onData).errorOption:
    return failure err

  return success exists

proc delete*(self: SQLiteDatastore, key: DbKey): ?!void =
  return self.db.deleteStmt.exec((key.data))

proc delete*(self: SQLiteDatastore, keys: openArray[DbKey]): ?!void =
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

  if err =? self.db.getStmt.query((key), onData).errorOption:
    return failure(err)

  if bytes.len <= 0:
    return failure(
      newException(DatastoreKeyNotFound, "DbKey doesn't exist"))

  return success bytes

proc put*(self: SQLiteDatastore, key: DbKey, data: DbVal): ?!void =
  when DbVal is seq[byte]:
    return self.db.putStmt.exec((key, data, timestamp()))
  elif DbVal is DataBuffer:
    return self.db.putBufferStmt.exec((key.id, data, timestamp()))
  else:
    {.error: "unknown type".}

proc put*(self: SQLiteDatastore, batch: openArray[DbBatchEntry]): ?!void =
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

proc query*(self: SQLiteDatastore,
            query: DbQuery
            ): Result[iterator(): ?!DbQueryResponse {.closure.}, ref CatchableError] =

  var
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
      s, 1.cint, ($query.key & "*").cstring, -1.cint, SQLITE_TRANSIENT_GCSAFE)

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

  success iterator(): ?!DbQueryResponse {.closure.} =

    try:
      let
        v = sqlite3_step(s)

      case v
      of SQLITE_ROW:
        let
          key = KeyId.new(sqlite3_column_text_not_null(s, QueryStmtIdCol))

          blob: ?pointer =
            if query.value: sqlite3_column_blob(s, QueryStmtDataCol).some
            else: pointer.none

        # detect out-of-memory error
        # see the conversion table and final paragraph of:
        # https://www.sqlite.org/c3ref/column_blob.html
        # see also https://www.sqlite.org/rescode.html

        # the "data" column can be NULL so in order to detect an out-of-memory
        # error it is necessary to check that the result is a null pointer and
        # that the result code is an error code
        if blob.isSome and blob.get().isNil:
          let v = sqlite3_errcode(sqlite3_db_handle(s))

          if not (v in [SQLITE_OK, SQLITE_ROW, SQLITE_DONE]):
            return failure newException(DatastoreError, $sqlite3_errstr(v))

        let
          dataLen = sqlite3_column_bytes(s, QueryStmtDataCol)
          data =
            if blob.isSome: DataBuffer.new(blob.get(), 0, dataLen - 1)
            else: DataBuffer.new(0)

        return success (key.some, data)
      of SQLITE_DONE:
        return
      else:
        return failure newException(DatastoreError, $sqlite3_errstr(v))

    finally:
      discard sqlite3_reset(s)
      discard sqlite3_clear_bindings(s)
      s.dispose()
      return


proc contains*(self: SQLiteDatastore, key: DbKey): bool =
  return self.has(key)


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
