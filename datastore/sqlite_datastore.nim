import std/os
import std/times

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/sqlite3_abi
import pkg/stew/byteutils
from pkg/stew/results as stewResults import get, isErr
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

  idColName = "id"
  dataColName = "data"
  timestampColName = "timestamp"

  idColIndex = 0
  dataColIndex = 1
  timestampColIndex = 2

  idColType = "TEXT"
  dataColType = "BLOB"
  timestampColType = "INTEGER"

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

  putStmtStr = """
    REPLACE INTO """ & tableName & """ (
      """ & idColName & """,
      """ & dataColName & """,
      """ & timestampColName & """
    ) VALUES (?, ?, ?);
  """

template checkColMetadata(s: RawStmtPtr, i: int, expectedName: string) =
  let
    colName = sqlite3_column_origin_name(s, i.cint)

  if colName.isNil:
    raise (ref Defect)(msg: "no column exists for index " & $i)

  if $colName != expectedName:
    raise (ref Defect)(msg: "original column name for index " & $i & " was \"" &
      $colName & "\" but expected \"" & expectedName & "\"")

proc idCol*(
  s: RawStmtPtr,
  index = idColIndex): BoundIdCol =

  checkColMetadata(s, index, idColName)

  return proc (): string =
    let
      text = sqlite3_column_text(s, index.cint)

    # detect out-of-memory error
    # see the conversion table and final paragraph of:
    # https://www.sqlite.org/c3ref/column_blob.html

    # the "id" column is NOT NULL PRIMARY KEY so an out-of-memory error can be
    # inferred from a null pointer result
    if text.isNil:
      let
        code = sqlite3_errcode(sqlite3_db_handle(s))

      raise (ref Defect)(msg: $sqlite3_errstr(code))

    $text.cstring

proc dataCol*(
  s: RawStmtPtr,
  index = dataColIndex): BoundDataCol =

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
        code = sqlite3_errcode(sqlite3_db_handle(s))

      if not (code in [SQLITE_OK, SQLITE_ROW, SQLITE_DONE]):
        raise (ref Defect)(msg: $sqlite3_errstr(code))

    let
      dataLen = sqlite3_column_bytes(s, i)

    # an out-of-memory error can be inferred from a null pointer result
    if (unsafeAddr dataLen).isNil:
      let
        code = sqlite3_errcode(sqlite3_db_handle(s))

      raise (ref Defect)(msg: $sqlite3_errstr(code))

    let
      dataBytes = cast[ptr UncheckedArray[byte]](blob)

    @(toOpenArray(dataBytes, 0, dataLen - 1))

proc timestampCol*(
  s: RawStmtPtr,
  index = timestampColIndex): BoundTimestampCol =

  checkColMetadata(s, index, timestampColName)

  return proc (): int64 =
    sqlite3_column_int64(s, index.cint)

proc new*(
  T: type SQLiteDatastore,
  basePath = "data",
  filename = "store" & dbExt,
  readOnly = false,
  inMemory = false): ?!T =

  # make it optional to enable WAL with it enabled being the default?

  # make it possible to specify a custom page size?
  # https://www.sqlite.org/pragma.html#pragma_page_size
  # https://www.sqlite.org/intern-v-extern-blob.html

  var
    env: AutoDisposed[SQLite]

  defer: disposeIfUnreleased(env)

  var
    basep, fname, dbPath: string

  if inMemory:
    if readOnly:
      return failure "SQLiteDatastore cannot be read-only and in-memory"
    else:
      dbPath = ":memory:"
  else:
    try:
      basep = normalizePathEnd(
        if basePath.isAbsolute: basePath
        else: getCurrentDir() / basePath)

      fname = filename.normalizePathEnd
      dbPath = basep / fname

      if readOnly and not fileExists(dbPath):
        return failure "read-only database does not exist: " & dbPath
      else:
        createDir(basep)

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
    pragmaStmt = journalModePragmaStmt(env.val)

  checkExec(pragmaStmt)

  var
    containsStmt: ContainsStmt
    deleteStmt: DeleteStmt
    getStmt: GetStmt
    putStmt: PutStmt

  if not readOnly:
    checkExec(env.val, createStmtStr)

    deleteStmt = ? DeleteStmt.prepare(env.val, deleteStmtStr)
    putStmt = ? PutStmt.prepare(env.val, putStmtStr)

  containsStmt = ? ContainsStmt.prepare(env.val, containsStmtStr)
  getStmt = ? GetStmt.prepare(env.val, getStmtStr)

  # if a readOnly/existing database does not satisfy the expected schema
  # `pepare()` will fail and `new` will return an error with message
  # "SQL logic error"

  let
    getDataCol = dataCol(RawStmtPtr(getStmt), 0)

  success T(dbPath: dbPath, containsStmt: containsStmt, deleteStmt: deleteStmt,
            env: env.release, getStmt: getStmt, getDataCol: getDataCol,
            putStmt: putStmt, readOnly: readOnly)

proc dbPath*(self: SQLiteDatastore): string =
  self.dbPath

proc env*(self: SQLiteDatastore): SQLite =
  self.env

proc close*(self: SQLiteDatastore) =
  self.containsStmt.dispose
  self.getStmt.dispose

  if not self.readOnly:
    self.deleteStmt.dispose
    self.putStmt.dispose

  self.env.dispose
  self[] = SQLiteDatastore()[]

proc timestamp*(t = epochTime()): int64 =
  (t * 1_000_000).int64

method contains*(
  self: SQLiteDatastore,
  key: Key): Future[?!bool] {.async, locks: "unknown".} =

  var
    exists = false

  proc onData(s: RawStmtPtr) =
    exists = sqlite3_column_int64(s, 0).bool

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

# method query*(
#   self: SQLiteDatastore,
#   query: ...): Future[?!(?...)] {.async, locks: "unknown".} =
#
#   return success ....some
