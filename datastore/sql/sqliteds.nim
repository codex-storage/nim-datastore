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
  SQLiteBackend* = object
    db: SQLiteDsDb

proc path*(self: SQLiteBackend): string =
  $self.db.dbPath

proc readOnly*(self: SQLiteBackend): bool = self.db.readOnly

proc timestamp*(t = epochTime()): int64 =
  (t * 1_000_000).int64

proc has*(self: SQLiteBackend, key: DbKey): ?!bool =
  var
    exists = false
    key = $key

  proc onData(s: RawStmtPtr) =
    exists = sqlite3_column_int64(s, ContainsStmtExistsCol.cint).bool

  if err =? self.db.containsStmt.query((key), onData).errorOption:
    return failure err

  return success exists

proc delete*(self: SQLiteBackend, key: DbKey): ?!void =
  return self.db.deleteStmt.exec(($key))

proc delete*(self: SQLiteBackend, keys: openArray[DbKey]): ?!void =
  if err =? self.db.beginStmt.exec().errorOption:
    return failure(err)

  for key in keys:
    if err =? self.db.deleteStmt.exec(($key)).errorOption:
      if err =? self.db.rollbackStmt.exec().errorOption:
        return failure err.msg

      return failure err.msg

  if err =? self.db.endStmt.exec().errorOption:
    return failure err.msg

  return success()

proc get*(self: SQLiteBackend, key: DbKey): ?!seq[byte] =
  # see comment in ./filesystem_datastore re: finer control of memory
  # allocation in `proc get`, could apply here as well if bytes were read
  # incrementally with `sqlite3_blob_read`

  var
    bytes: seq[byte]

  proc onData(s: RawStmtPtr) =
    bytes = dataCol(self.db.getDataCol)

  if err =? self.db.getStmt.query(($key), onData).errorOption:
    return failure(err)

  if bytes.len <= 0:
    return failure(
      newException(DatastoreKeyNotFound, "DbKey doesn't exist"))

  return success bytes

proc put*(self: SQLiteBackend, key: DbKey, data: DbVal): ?!void =
  when DbVal is seq[byte]:
    return self.db.putStmt.exec((key, data, timestamp()))
  elif DbVal is DataBuffer:
    return self.db.putBufferStmt.exec((key, data, timestamp()))
  else:
    {.error: "unknown type".}

proc put*(self: SQLiteBackend, batch: openArray[DbBatchEntry]): ?!void =
  if err =? self.db.beginStmt.exec().errorOption:
    return failure err

  for entry in batch:
    # DbBatchEntry* = tuple[key: string, data: seq[byte]] | tuple[key: KeyId, data: DataBuffer]
    when entry.key is string:
      let putStmt = self.db.putStmt
    elif entry.key is KeyId:
      let putStmt = self.db.putBufferStmt
    else:
      {.error: "unhandled type".}
    if err =? putStmt.exec((entry.key, entry.data, timestamp())).errorOption:
      if err =? self.db.rollbackStmt.exec().errorOption:
        return failure err

      return failure err

  if err =? self.db.endStmt.exec().errorOption:
    return failure err

  return success()

proc close*(self: SQLiteBackend): ?!void =
  self.db.close()

  return success()

proc query*(
    self: SQLiteBackend,
    query: DbQuery
): Result[(DbQueryHandle, iterator(): ?!DbQueryResponse {.closure.}),
          ref CatchableError] =

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
    queryKey = $query.key & "*"

  var
    v = sqlite3_bind_text(
      s, 1.cint, queryKey.cstring, queryKey.len().cint, SQLITE_TRANSIENT_GCSAFE)

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

  proc doClose() =
      echo "sqlite backend: query: finally close"
      discard sqlite3_reset(s)
      discard sqlite3_clear_bindings(s)
      s.dispose()
      return

  let handle = DbQueryHandle()
  let iter = iterator(): ?!DbQueryResponse {.closure.} =

    while true:

      if handle.cancel:
        doClose()
        return

      let v = sqlite3_step(s)

      case v
      of SQLITE_ROW:
        echo "SQLITE ROW"
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
            if blob.isSome:
              let arr = cast[ptr UncheckedArray[byte]](blob)
              DataBuffer.new(arr.toOpenArray(0, dataLen-1))
            else: DataBuffer.new("")

        echo "SQLITE ROW: yield"
        yield success (key.some, data)
      of SQLITE_DONE:
        echo "SQLITE DONE: return"
        doClose()
        return
      else:
        echo "SQLITE ERROR: return"
        doClose()
        return failure newException(DatastoreError, $sqlite3_errstr(v))
    
  success (handle, iter)


proc contains*(self: SQLiteBackend, key: DbKey): bool =
  return self.has(key).get()


proc new*(T: type SQLiteBackend,
          path: string,
          readOnly = false): ?!SQLiteBackend =

  let
    flags =
      if readOnly: SQLITE_OPEN_READONLY
      else: SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE

  success SQLiteBackend(db: ? SQLiteDsDb.open(path, flags))
    

proc new*(T: type SQLiteBackend,
          db: SQLiteDsDb): ?!T =

  success SQLiteBackend(db: db)
