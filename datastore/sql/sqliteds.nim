{.push raises: [].}

import std/times
import std/options

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/sqlite3_abi

import ../datastore
import ./sqlitedsdb
import ./sqliteutils

export datastore, sqlitedsdb

type
  SQLiteDatastore* = ref object of Datastore
    readOnly: bool
    db: SQLiteDsDb

proc path*(self: SQLiteDatastore): string =
  self.db.dbPath

proc `readOnly=`*(self: SQLiteDatastore): bool
  {.error: "readOnly should not be assigned".}

proc timestamp*(t = epochTime()): int64 =
  (t * 1_000_000).int64

const initVersion* = 0.int64

type RollbackError* = object of CatchableError

proc newRollbackError(rbErr: ref CatchableError, opErrMsg: string): ref RollbackError =
  let
    msg = "Rollback initiated because of: " & opErrMsg & ". Rollback failed because of: " & rbErr.msg
  return newException(RollbackError, msg, parentException = rbErr)

proc newRollbackError(rbErr: ref CatchableError, opErr: ref CatchableError): ref RollbackError =
  return newRollbackError(rbErr, opErr)

method modifyGet*(self: SQLiteDatastore, key: Key, fn: ModifyGet): Future[?!seq[byte]] {.async.} =
  var
    retriesLeft = 100 # allows reasonable concurrency, avoids infinite loop
    aux: seq[byte]

  while retriesLeft > 0:
    var
      currentData: seq[byte]
      currentVersion: int64

    proc onData(s: RawStmtPtr) =
      currentData = dataCol(s, GetVersionedStmtDataCol)()
      currentVersion = versionCol(s, GetVersionedStmtVersionCol)()

    if err =? self.db.getVersionedStmt.query((key.id), onData).errorOption:
      return failure(err)

    let maybeCurrentData = if currentData.len > 0: some(currentData) else: seq[byte].none
    var maybeNewData: ?seq[byte]

    try:
      (maybeNewData, aux) = await fn(maybeCurrentData)
    except CatchableError as err:
      return failure(err)

    if maybeCurrentData == maybeNewData:
      # no need to change currently stored value
      break

    if err =? self.db.beginStmt.exec().errorOption:
      return failure(err)
    if currentData =? maybeCurrentData and newData =? maybeNewData:
      let updateParams = (
        newData,
        currentVersion + 1,
        timestamp(),
        key.id,
        currentVersion
      )
      if err =? (self.db.updateVersionedStmt.exec(updateParams)).errorOption:
        if rbErr =? self.db.rollbackStmt.exec().errorOption:
          return failure(newRollbackError(rbErr, err))
        return failure(err)
    elif currentData =? maybeCurrentData:
      let deleteParams = (
        key.id,
        currentVersion
      )
      if err =? (self.db.deleteVersionedStmt.exec(deleteParams)).errorOption:
        if rbErr =? self.db.rollbackStmt.exec().errorOption:
          return failure(newRollbackError(rbErr, err))
        return failure(err)
    elif newData =? maybeNewData:
      let insertParams = (
        key.id,
        newData,
        initVersion,
        timestamp()
      )
      if err =? (self.db.insertVersionedStmt.exec(insertParams)).errorOption:
        if rbErr =? self.db.rollbackStmt.exec().errorOption:
          return failure(newRollbackError(rbErr, err))
        return failure(err)

    var changes = 0.int64
    proc onChangesResult(s: RawStmtPtr) =
      changes = changesCol(s, 0)()

    if err =? self.db.getChangesStmt.query((), onChangesResult).errorOption:
      if rbErr =? self.db.rollbackStmt.exec().errorOption:
        return failure(newRollbackError(rbErr, err))
      return failure(err)

    if changes == 1:
      if err =? self.db.endStmt.exec().errorOption:
        if rbErr =? self.db.rollbackStmt.exec().errorOption:
          return failure(newRollbackError(rbErr, err))
        return failure(err)
      break
    elif changes == 0:
      if rbErr =? self.db.rollbackStmt.exec().errorOption:
        return failure(newRollbackError(rbErr, "Unable to retry after race condition was detected"))
      retriesLeft.dec
    else:
      let msg = "Unexpected number of changes, expected either 0 or 1, was " & $changes
      if rbErr =? self.db.rollbackStmt.exec().errorOption:
        return failure(newRollbackError(rbErr, msg))
      return failure(msg)

  if retriesLeft == 0:
    return failure("Retry limit exceeded")

  return success(aux)


method modify*(self: SQLiteDatastore, key: Key, fn: Modify): Future[?!void] {.async.} =
  proc wrappedFn(maybeValue: ?seq[byte]): Future[(?seq[byte], seq[byte])] {.async.} =
    let res = await fn(maybeValue)
    let ignoredAux = newSeq[byte]()
    return (res, ignoredAux)

  if err =? (await self.modifyGet(key, wrappedFn)).errorOption:
    return failure(err)
  else:
    return success()

method has*(self: SQLiteDatastore, key: Key): Future[?!bool] {.async.} =
  var
    exists = false

  proc onData(s: RawStmtPtr) =
    exists = sqlite3_column_int64(s, ContainsStmtExistsCol.cint).bool

  if err =? self.db.containsStmt.query((key.id), onData).errorOption:
    return failure err

  return success exists

method delete*(self: SQLiteDatastore, key: Key): Future[?!void] {.async.} =
  return self.db.deleteStmt.exec((key.id))

method delete*(self: SQLiteDatastore, keys: seq[Key]): Future[?!void] {.async.} =
  if err =? self.db.beginStmt.exec().errorOption:
    return failure(err)

  for key in keys:
    if err =? self.db.deleteStmt.exec((key.id)).errorOption:
      if rbErr =? self.db.rollbackStmt.exec().errorOption:
        return failure(newRollbackError(rbErr, err))

      return failure err.msg

  if err =? self.db.endStmt.exec().errorOption:
    return failure err.msg

  return success()

method get*(self: SQLiteDatastore, key: Key): Future[?!seq[byte]] {.async.} =
  # see comment in ./filesystem_datastore re: finer control of memory
  # allocation in `method get`, could apply here as well if bytes were read
  # incrementally with `sqlite3_blob_read`

  var
    bytes: seq[byte]

  proc onData(s: RawStmtPtr) =
    bytes = self.db.getDataCol()

  if err =? self.db.getStmt.query((key.id), onData).errorOption:
    return failure(err)

  if bytes.len <= 0:
    return failure(
      newException(DatastoreKeyNotFound, "Key doesn't exist"))

  return success bytes

method put*(self: SQLiteDatastore, key: Key, data: seq[byte]): Future[?!void] {.async.} =
  return self.db.putStmt.exec((key.id, data, initVersion, timestamp()))

method put*(self: SQLiteDatastore, batch: seq[BatchEntry]): Future[?!void] {.async.} =
  if err =? self.db.beginStmt.exec().errorOption:
    return failure err

  for entry in batch:
    if err =? self.db.putStmt.exec((entry.key.id, entry.data, initVersion, timestamp())).errorOption:
      if rbErr =? self.db.rollbackStmt.exec().errorOption:
        return failure(newRollbackError(rbErr, err))

      return failure err

  if err =? self.db.endStmt.exec().errorOption:
    return failure err

  return success()

method close*(self: SQLiteDatastore): Future[?!void] {.async.} =
  self.db.close()

  return success()

method query*(
  self: SQLiteDatastore,
  query: Query): Future[?!QueryIter] {.async.} =

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
      self.db.env, queryStr).expect("Query prepare should not fail")
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
          $sqlite3_column_text_not_null(s, QueryStmtIdCol))
          .expect("Key should should not fail")

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
      return success (Key.none, EmptyBytes)
    else:
      iter.finished = true
      return failure newException(DatastoreError, $sqlite3_errstr(v))

  iter.dispose = proc(): Future[?!void] {.async.} =
    discard sqlite3_reset(s)
    discard sqlite3_clear_bindings(s)
    iter.next = nil
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
    db: ? SQLiteDsDb.open(path, flags),
    readOnly: readOnly)

proc new*(
  T: type SQLiteDatastore,
  db: SQLiteDsDb): ?!T =

  success T(
    db: db,
    readOnly: db.readOnly)
