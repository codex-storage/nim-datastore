import std/times

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/sqlite3_abi
from pkg/stew/results as stewResults import isErr
import pkg/upraises

import ./datastore
import ./sqlitedsdb

export datastore, sqlitedsdb

push: {.upraises: [].}

const
  EmptyBytes = newSeq[byte](0)

type
  SQLiteDatastore* = ref object of Datastore
    readOnly: bool
    db: SQLiteDsDB

proc path*(self: SQLiteDatastore): string =
  self.db.dbPath

proc `readOnly=`*(self: SQLiteDatastore): bool
  {.error: "readOnly should not be assigned".}

proc timestamp*(t = epochTime()): int64 =
  (t * 1_000_000).int64

method contains*(self: SQLiteDatastore, key: Key): Future[?!bool] {.async.} =
  var
    exists = false

  proc onData(s: RawStmtPtr) =
    exists = sqlite3_column_int64(s, ContainsStmtExistsCol.cint).bool

  if (
    let res = self.db.containsStmt.query((key.id), onData);
    res.isErr):
    return failure res.error.msg

  return success exists

method delete*(self: SQLiteDatastore, key: Key): Future[?!void] {.async.} =
  return self.db.deleteStmt.exec((key.id))

method get*(self: SQLiteDatastore, key: Key): Future[?!seq[byte]] {.async.} =
  # see comment in ./filesystem_datastore re: finer control of memory
  # allocation in `method get`, could apply here as well if bytes were read
  # incrementally with `sqlite3_blob_read`

  var
    bytes: seq[byte]

  proc onData(s: RawStmtPtr) =
    bytes = self.db.getDataCol()

  if (
    let res = self.db.getStmt.query((key.id), onData);
    res.isErr):
    return failure res.error.msg

  return success bytes

method put*(self: SQLiteDatastore, key: Key, data: seq[byte]): Future[?!void] {.async.} =
  return self.db.putStmt.exec((key.id, data, timestamp()))

method query*(
  self: SQLiteDatastore,
  query: Query): Future[?!QueryIter] {.async.} =

  var
    iter = QueryIter()
    queryStr = QueryStmtStr

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

  proc next(): Future[?!QueryResponse] {.async.} =
    if iter.finished:
      return failure(newException(QueryEndedError, "Calling next on a finished query!"))

    let
      v = sqlite3_step(s)

    case v
    of SQLITE_ROW:
      let
        key = Key.init(
          $sqlite3_column_text_not_null(s,QueryStmtIdCol))
          .expect("should not fail")

        blob = sqlite3_column_blob(s, QueryStmtDataCol)

      # detect out-of-memory error
      # see the conversion table and final paragraph of:
      # https://www.sqlite.org/c3ref/column_blob.html
      # see also https://www.sqlite.org/rescode.html

      # the "data" column can be NULL so in order to detect an out-of-memory
      # error it is necessary to check that the result is a null pointer and
      # that the result code is an error code
      if blob.isNil:
        let
          v = sqlite3_errcode(sqlite3_db_handle(s))

        if not (v in [SQLITE_OK, SQLITE_ROW, SQLITE_DONE]):
          iter.finished = true
          return failure newException(DatastoreError, $sqlite3_errstr(v))

      let
        dataLen = sqlite3_column_bytes(s, QueryStmtDataCol)
        dataBytes = cast[ptr UncheckedArray[byte]](blob)
        data = @(toOpenArray(dataBytes, 0, dataLen - 1))

      return success (key.some, data)
    of SQLITE_DONE:
      iter.finished = true
      return success (Key.none, EmptyBytes)
    else:
      iter.finished = true
      return failure newException(DatastoreError, $sqlite3_errstr(v))

  iter.dispose = proc(): Future[?!void] {.async.} =
    discard sqlite3_reset(s)
    discard sqlite3_clear_bindings(s)
    s.dispose
    return success()

  iter.next = next
  return success iter

proc new*(
  T: type SQLiteDatastore,
  path: string,
  readOnly = false): ?!T =

  let
    flags =
      if readOnly: SQLITE_OPEN_READONLY
      else: SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE

  success T(
    db: ? SQLIteDsDb.open(path, flags),
    readOnly: readOnly)

proc new*(
  T: type SQLiteDatastore,
  db: SQLIteDsDb): ?!T =

  success T(
    db: db,
    readOnly: db.readOnly)
