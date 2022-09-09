import std/os
import std/times

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/sqlite3_abi
import pkg/stew/byteutils
from pkg/stew/results as stewResults import isErr
import pkg/upraises

import ./datastore
import ./sqlite

export datastore, sqlite

push: {.upraises: [].}

type
  BoundIdCol = proc (): string {.closure, gcsafe, upraises: [].}

  BoundDataCol = proc (): seq[byte] {.closure, gcsafe, upraises: [].}

  BoundTimestampCol = proc (): int64 {.closure, gcsafe, upraises: [].}

  # feels odd to use `void` for prepared statements corresponding to SELECT
  # queries but it fits with the rest of the SQLite wrapper adapted from
  # status-im/nwaku, at least in its current form in ./sqlite
  ContainsStmt = SQLiteStmt[(string), void]

  DeleteStmt = SQLiteStmt[(string), void]

  GetStmt = SQLiteStmt[(string), void]

  PutStmt = SQLiteStmt[(string, seq[byte], int64), void]

  QueryStmt = SQLiteStmt[(string), void]

  SQLiteDatastore* = ref object of Datastore
    dbPath: string
    containsStmt: ContainsStmt
    deleteStmt: DeleteStmt
    env: SQLite
    getDataCol: BoundDataCol
    getStmt: GetStmt
    putStmt: PutStmt
    readOnly: bool

const
  dbExt* = ".sqlite3"
  tableName* = "Store"

  idColName* = "id"
  dataColName* = "data"
  timestampColName* = "timestamp"

  idColType = "TEXT"
  dataColType = "BLOB"
  timestampColType = "INTEGER"

  memory* = ":memory:"

  # https://stackoverflow.com/a/9756276
  # EXISTS returns a boolean value represented by an integer:
  # https://sqlite.org/datatype3.html#boolean_datatype
  # https://sqlite.org/lang_expr.html#the_exists_operator
  containsStmtStr = """
    SELECT EXISTS(
      SELECT 1 FROM """ & tableName & """
      WHERE """ & idColName & """ = ?
    );
  """

  containsStmtExistsCol = 0

  createStmtStr = """
    CREATE TABLE IF NOT EXISTS """ & tableName & """ (
      """ & idColName & """ """ & idColType & """ NOT NULL PRIMARY KEY,
      """ & dataColName & """ """ & dataColType & """,
      """ & timestampColName & """ """ & timestampColType & """ NOT NULL
    ) WITHOUT ROWID;
  """

  deleteStmtStr = """
    DELETE FROM """ & tableName & """
    WHERE """ & idColName & """ = ?;
  """

  getStmtStr = """
    SELECT """ & dataColName & """ FROM """ & tableName & """
    WHERE """ & idColName & """ = ?;
  """

  getStmtDataCol = 0

  putStmtStr = """
    REPLACE INTO """ & tableName & """ (
      """ & idColName & """,
      """ & dataColName & """,
      """ & timestampColName & """
    ) VALUES (?, ?, ?);
  """

  queryStmtStr = """
    SELECT """ & idColName & """, """ & dataColName & """ FROM """ & tableName &
    """ WHERE """ & idColName & """ GLOB ?;
  """

  queryStmtIdCol = 0
  queryStmtDataCol = 1

proc checkColMetadata(s: RawStmtPtr, i: int, expectedName: string) =
  let
    colName = sqlite3_column_origin_name(s, i.cint)

  if colName.isNil:
    raise (ref Defect)(msg: "no column exists for index " & $i & " in `" &
      $sqlite3_sql(s) & "`")

  if $colName != expectedName:
    raise (ref Defect)(msg: "original column name for index " & $i & " was \"" &
      $colName & "\" in `" & $sqlite3_sql(s) & "` but callee expected \"" &
      expectedName & "\"")

proc idCol*(
  s: RawStmtPtr,
  index: int): BoundIdCol =

  checkColMetadata(s, index, idColName)

  return proc (): string =
    $sqlite3_column_text_not_null(s, index.cint)

proc dataCol*(
  s: RawStmtPtr,
  index: int): BoundDataCol =

  checkColMetadata(s, index, dataColName)

  return proc (): seq[byte] =
    let
      i = index.cint
      blob = sqlite3_column_blob(s, i)

    # detect out-of-memory error
    # see the conversion table and final paragraph of:
    # https://www.sqlite.org/c3ref/column_blob.html
    # see also https://www.sqlite.org/rescode.html

    # the "data" column can be NULL so in order to detect an out-of-memory error
    # it is necessary to check that the result is a null pointer and that the
    # result code is an error code
    if blob.isNil:
      let
        v = sqlite3_errcode(sqlite3_db_handle(s))

      if not (v in [SQLITE_OK, SQLITE_ROW, SQLITE_DONE]):
        raise (ref Defect)(msg: $sqlite3_errstr(v))

    let
      dataLen = sqlite3_column_bytes(s, i)
      dataBytes = cast[ptr UncheckedArray[byte]](blob)

    @(toOpenArray(dataBytes, 0, dataLen - 1))

proc timestampCol*(
  s: RawStmtPtr,
  index: int): BoundTimestampCol =

  checkColMetadata(s, index, timestampColName)

  return proc (): int64 =
    sqlite3_column_int64(s, index.cint)

proc new*(
  T: type SQLiteDatastore,
  basePath: string,
  filename = "store" & dbExt,
  readOnly = false,

  # SQLite's default page_size is 4096 bytes since v3.12.0 (2016-03-29)
  # https://www.sqlite.org/pragma.html#pragma_page_size
  # see also: https://www.sqlite.org/intern-v-extern-blob.html
  pageSize: Positive = 4096,

  # SQLite's default cache_size is -2000 since v3.12.0 (2016-03-29)
  # a negative value translates to approximately "abs(cache_size*1024) bytes of memory"
  # a positive value translates to "cache_size*page_size bytes of memory"
  # docs for `PRAGMA cache_size` may need some clarification
  # https://www.sqlite.org/pragma.html#pragma_cache_size
  # https://www.sqlite.org/pgszchng2016.html
  # https://sqlite.org/forum/forumpost/096a95c0f9
  # NOTE: a system build may have used nonstandard compile-time options,
  # e.g. in recent versions of macOS the default cache_size is (positive) 2000
  cacheSize = -2000,

  # https://www.sqlite.org/pragma.html#pragma_journal_mode
  journalMode = WAL): ?!T =

  var
    env: AutoDisposed[SQLite]

  defer: disposeIfUnreleased(env)

  var
    basep, fname, dbPath: string

  if basePath == memory:
    if readOnly:
      return failure "SQLiteDatastore cannot be read-only and in-memory"
    else:
      dbPath = memory
  else:
    try:
      basep = normalizePathEnd(
        if basePath.isAbsolute: basePath
        else: getCurrentDir() / basePath)

      fname = filename.normalizePathEnd
      dbPath = basep / fname

      if readOnly and not fileExists(dbPath):
        return failure "read-only database does not exist: " & dbPath
      elif not dirExists(basep):
        return failure "directory does not exist: " & basep

    except IOError as e:
      return failure e

    except OSError as e:
      return failure e

  let
    flags =
      if readOnly: SQLITE_OPEN_READONLY
      else: SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE

  open(dbPath, env.val, flags)

  let
    pageSizePragmaStmt = pageSizePragmaStmt(env.val, pageSize)
    cacheSizePragmaStmt = cacheSizePragmaStmt(env.val, cacheSize)
    journalModePragmaStmt = journalModePragmaStmt(env.val, journalMode)

  checkExec(pageSizePragmaStmt)
  checkExec(cacheSizePragmaStmt)
  checkExec(journalModePragmaStmt)

  var
    containsStmt: ContainsStmt
    deleteStmt: DeleteStmt
    getStmt: GetStmt
    putStmt: PutStmt

  if not readOnly:
    checkExec(env.val, createStmtStr)

    deleteStmt = ? DeleteStmt.prepare(
      env.val, deleteStmtStr, SQLITE_PREPARE_PERSISTENT)

    putStmt = ? PutStmt.prepare(
      env.val, putStmtStr, SQLITE_PREPARE_PERSISTENT)

  containsStmt = ? ContainsStmt.prepare(
    env.val, containsStmtStr, SQLITE_PREPARE_PERSISTENT)

  getStmt = ? GetStmt.prepare(
    env.val, getStmtStr, SQLITE_PREPARE_PERSISTENT)

  # if a readOnly/existing database does not satisfy the expected schema
  # `pepare()` will fail and `new` will return an error with message
  # "SQL logic error"

  let
    getDataCol = dataCol(RawStmtPtr(getStmt), getStmtDataCol)

  success T(dbPath: dbPath, containsStmt: containsStmt, deleteStmt: deleteStmt,
            env: env.release, getStmt: getStmt, getDataCol: getDataCol,
            putStmt: putStmt, readOnly: readOnly)

proc dbPath*(self: SQLiteDatastore): string =
  self.dbPath

proc env*(self: SQLiteDatastore): SQLite =
  self.env

proc timestamp*(t = epochTime()): int64 =
  (t * 1_000_000).int64

method close*(self: SQLiteDatastore) {.async, locks: "unknown".} =
  self.containsStmt.dispose
  self.getStmt.dispose

  if not self.readOnly:
    self.deleteStmt.dispose
    self.putStmt.dispose

  self.env.dispose
  self[] = SQLiteDatastore()[]

method contains*(
  self: SQLiteDatastore,
  key: Key): Future[?!bool] {.async, locks: "unknown".} =

  var
    exists = false

  proc onData(s: RawStmtPtr) =
    exists = sqlite3_column_int64(s, containsStmtExistsCol.cint).bool

  let
    queryRes = self.containsStmt.query((key.id), onData)

  if queryRes.isErr: return queryRes

  return success exists

method delete*(
  self: SQLiteDatastore,
  key: Key): Future[?!void] {.async, locks: "unknown".} =

  if self.readOnly:
    return failure "database is read-only":
  else:
    return self.deleteStmt.exec((key.id))

method get*(
  self: SQLiteDatastore,
  key: Key): Future[?!(?seq[byte])] {.async, locks: "unknown".} =

  # see comment in ./filesystem_datastore re: finer control of memory
  # allocation in `method get`, could apply here as well if bytes were read
  # incrementally with `sqlite3_blob_read`

  var
    bytes: ?seq[byte]

  let
    dataCol = self.getDataCol

  proc onData(s: RawStmtPtr) =
    bytes = dataCol().some

  let
    queryRes = self.getStmt.query((key.id), onData)

  if queryRes.isErr:
    return failure queryRes.error.msg
  else:
    return success bytes

proc put*(
  self: SQLiteDatastore,
  key: Key,
  data: seq[byte],
  timestamp: int64): Future[?!void] {.async.} =

  if self.readOnly:
    return failure "database is read-only"
  else:
    return self.putStmt.exec((key.id, @data, timestamp))

method put*(
  self: SQLiteDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async, locks: "unknown".} =

  return await self.put(key, data, timestamp())

iterator query*(
  self: SQLiteDatastore,
  query: Query): Future[QueryResponse] =

  let
    queryStmt = QueryStmt.prepare(
      self.env, queryStmtStr).expect("should not fail")

    s = RawStmtPtr(queryStmt)

  defer:
    discard sqlite3_reset(s)
    discard sqlite3_clear_bindings(s)
    s.dispose

  let
    v = sqlite3_bind_text(s, 1.cint, query.key.id.cstring, -1.cint,
      SQLITE_TRANSIENT_GCSAFE)

  if not (v == SQLITE_OK):
    raise (ref Defect)(msg: $sqlite3_errstr(v))

  while true:
    let
      v = sqlite3_step(s)

    case v
    of SQLITE_ROW:
      let
        key = Key.init($sqlite3_column_text_not_null(
          s, queryStmtIdCol)).expect("should not fail")

        blob = sqlite3_column_blob(s, queryStmtDataCol)

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
          raise (ref Defect)(msg: $sqlite3_errstr(v))

      let
        dataLen = sqlite3_column_bytes(s, queryStmtDataCol)
        dataBytes = cast[ptr UncheckedArray[byte]](blob)
        data = @(toOpenArray(dataBytes, 0, dataLen - 1))
        fut = newFuture[QueryResponse]()

      fut.complete((key, data))
      yield fut
    of SQLITE_DONE:
      break
    else:
      raise (ref Defect)(msg: $sqlite3_errstr(v))
