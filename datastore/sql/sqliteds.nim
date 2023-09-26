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
  SQLiteBackend*[K: DbKey, V: DbVal] = object
    db: SQLiteDsDb[K, V]

proc path*[K,V](self: SQLiteBackend[K,V]): string =
  $self.db.dbPath

proc readOnly*[K,V](self: SQLiteBackend[K,V]): bool = self.db.readOnly

proc timestamp*(t = epochTime()): int64 =
  (t * 1_000_000).int64

proc has*[K,V](self: SQLiteBackend[K,V], key: DbKey): ?!bool =
  var
    exists = false
    key = key

  proc onData(s: RawStmtPtr) =
    exists = sqlite3_column_int64(s, ContainsStmtExistsCol.cint).bool

  if err =? self.db.containsStmt.query((key), onData).errorOption:
    return failure err

  return success exists

proc delete*[K,V](self: SQLiteBackend[K,V], key: K): ?!void =
  return self.db.deleteStmt.exec((key))

proc delete*[K,V](self: SQLiteBackend[K,V], keys: openArray[DbKey]): ?!void =
  if err =? self.db.beginStmt.exec().errorOption:
    return failure(err)

  for key in keys:
    if err =? self.db.deleteStmt.exec((key)).errorOption:
      if err =? self.db.rollbackStmt.exec().errorOption:
        return failure err.msg

      return failure err.msg

  if err =? self.db.endStmt.exec().errorOption:
    return failure err.msg

  return success()

proc get*[K,V](self: SQLiteBackend[K,V], key: K): ?!seq[byte] =
  # see comment in ./filesystem_datastore re: finer control of memory
  # allocation in `proc get`, could apply here as well if bytes were read
  # incrementally with `sqlite3_blob_read`

  var
    bytes: seq[byte]

  proc onData(s: RawStmtPtr) =
    bytes = dataCol[V](self.db.getDataCol)

  if err =? self.db.getStmt.query((key), onData).errorOption:
    return failure(err)

  if bytes.len <= 0:
    return failure(
      newException(DatastoreKeyNotFound, "DbKey doesn't exist"))

  return success bytes

proc put*[K,V](self: SQLiteBackend[K,V], key: K, data: V): ?!void =
  return self.db.putStmt.exec((key, data, timestamp()))

proc put*[K,V](self: SQLiteBackend[K,V], batch: openArray[DbBatchEntry]): ?!void =
  if err =? self.db.beginStmt.exec().errorOption:
    return failure err

  for entry in batch:
    let putStmt = self.db.putStmt
    if err =? putStmt.exec((entry.key, entry.data, timestamp())).errorOption:
      if err =? self.db.rollbackStmt.exec().errorOption:
        return failure err

      return failure err

  if err =? self.db.endStmt.exec().errorOption:
    return failure err

  return success()

proc close*[K,V](self: SQLiteBackend[K,V]): ?!void =
  self.db.close()

  return success()

proc query*[K,V](
    self: SQLiteBackend[K,V],
    query: DbQuery
): Result[DbQueryHandle[K,V,RawStmtPtr], ref CatchableError] =

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

  success DbQueryHandle[K,V,RawStmtPtr](query: query, env: s)

proc close*[K,V](handle: var DbQueryHandle[K,V,RawStmtPtr]) =
  if not handle.closed:
    handle.closed = true
    discard sqlite3_reset(handle.env)
    discard sqlite3_clear_bindings(handle.env)
    handle.env.dispose()

iterator iter*[K, V](handle: var DbQueryHandle[K, V, RawStmtPtr]): ?!DbQueryResponse[K, V] =
  while not handle.cancel:

    let v = sqlite3_step(handle.env)

    case v
    of SQLITE_ROW:
      let
        key = K.toKey(sqlite3_column_text_not_null(handle.env, QueryStmtIdCol))

        blob: ?pointer =
          if handle.query.value: sqlite3_column_blob(handle.env, QueryStmtDataCol).some
          else: pointer.none

      # detect out-of-memory error
      # see the conversion table and final paragraph of:
      # https://www.sqlite.org/c3ref/column_blob.html
      # see also https://www.sqlite.org/rescode.html

      # the "data" column can be NULL so in order to detect an out-of-memory
      # error it is necessary to check that the result is a null pointer and
      # that the result code is an error code
      if blob.isSome and blob.get().isNil:
        let v = sqlite3_errcode(sqlite3_db_handle(handle.env))

        if not (v in [SQLITE_OK, SQLITE_ROW, SQLITE_DONE]):
          handle.cancel = true
          yield DbQueryResponse[K,V].failure newException(DatastoreError, $sqlite3_errstr(v))

      let
        dataLen = sqlite3_column_bytes(handle.env, QueryStmtDataCol)
        data =
          if blob.isSome:
            let arr = cast[ptr UncheckedArray[byte]](blob)
            V.toVal(arr.toOpenArray(0, dataLen-1))
          else: DataBuffer.new("")

      yield success (key.some, data)
    of SQLITE_DONE:
      handle.close()
      break
    else:
      handle.cancel = true
      yield DbQueryResponse[K,V].failure newException(DatastoreError, $sqlite3_errstr(v))
      break

  handle.close()


proc contains*[K,V](self: SQLiteBackend[K,V], key: DbKey): bool =
  return self.has(key).get()


proc newSQLiteBackend*[K,V](
          path: string,
          readOnly = false): ?!SQLiteBackend[K,V] =

  let
    flags =
      if readOnly: SQLITE_OPEN_READONLY
      else: SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE

  success SQLiteBackend[K,V](db: ? SQLiteDsDb[K,V].open(path, flags))
    

proc newSQLiteBackend*[K,V](
          db: SQLiteDsDb[K,V]): ?!SQLiteBackend[K,V] =

  success SQLiteBackend[K,V](db: db)
