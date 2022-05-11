import std/os
import std/times

import pkg/questionable
import pkg/questionable/results
import pkg/sqlite3_abi
import pkg/stew/byteutils
import pkg/upraises

import ./datastore
import ./sqlite

export datastore, sqlite

push: {.upraises: [].}

type
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
    getStmt: GetStmt
    putStmt: PutStmt
    readOnly: bool

const
  IdType = "TEXT"
  DataType = "BLOB"
  TimestampType = "INTEGER"

  dbExt* = ".sqlite3"
  tableTitle* = "Store"

  # https://stackoverflow.com/a/9756276
  # EXISTS returns a boolean value represented by an integer:
  # https://sqlite.org/datatype3.html#boolean_datatype
  # https://sqlite.org/lang_expr.html#the_exists_operator
  containsStmtStr = """
    SELECT EXISTS(
      SELECT 1 FROM """ & tableTitle & """
      WHERE id = ?
    );
  """

  createStmtStr = """
    CREATE TABLE IF NOT EXISTS """ & tableTitle & """ (
      id """ & IdType & """ NOT NULL PRIMARY KEY,
      data """ & DataType & """,
      timestamp """ & TimestampType & """ NOT NULL
    ) WITHOUT ROWID;
  """

  deleteStmtStr = """
    DELETE FROM """ & tableTitle & """
    WHERE id = ?;
  """

  getStmtStr = """
    SELECT data FROM """ & tableTitle & """
    WHERE id = ?;
  """

  putStmtStr = """
    REPLACE INTO """ & tableTitle & """ (
      id, data, timestamp
    ) VALUES (?, ?, ?);
  """

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

  success T(dbPath: dbPath, containsStmt: containsStmt, deleteStmt: deleteStmt,
            env: env.release, getStmt: getStmt, putStmt: putStmt,
            readOnly: readOnly)

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

proc idCol*(
  s: RawStmtPtr,
  index = 0): string =

  $sqlite3_column_text(s, index.cint).cstring

proc dataCol*(
  s: RawStmtPtr,
  index = 1): seq[byte] =

  let
    i = index.cint
    dataBytes = cast[ptr UncheckedArray[byte]](sqlite3_column_blob(s, i))
    dataLen = sqlite3_column_bytes(s, i)

  @(toOpenArray(dataBytes, 0, dataLen - 1))

proc timestampCol*(
  s: RawStmtPtr,
  index = 2): int64 =

  sqlite3_column_int64(s, index.cint)

method contains*(
  self: SQLiteDatastore,
  key: Key): ?!bool {.locks: "unknown".} =

  var
    exists = false

  proc onData(s: RawStmtPtr) {.closure.} =
    exists = sqlite3_column_int64(s, 0).bool

  discard ? self.containsStmt.query((key.id), onData)

  success exists

method delete*(
  self: SQLiteDatastore,
  key: Key): ?!void {.locks: "unknown".} =

  if self.readOnly:
    failure "database is read-only":
  else:
    self.deleteStmt.exec((key.id))

method get*(
  self: SQLiteDatastore,
  key: Key): ?!(?seq[byte]) {.locks: "unknown".} =

  # see comment in ./filesystem_datastore re: finer control of memory
  # allocation in `method get`, could apply here as well if bytes were read
  # incrementally with `sqlite3_blob_read`

  var
    bytes: seq[byte]

  proc onData(s: RawStmtPtr) {.closure.} =
    bytes = dataCol(s, 0)

  let
    exists = ? self.getStmt.query((key.id), onData)

  if exists:
    success bytes.some
  else:
    success seq[byte].none

proc put*(
  self: SQLiteDatastore,
  key: Key,
  data: openArray[byte],
  timestamp: int64): ?!void =

  if self.readOnly:
    failure "database is read-only"
  else:
    self.putStmt.exec((key.id, @data, timestamp))

method put*(
  self: SQLiteDatastore,
  key: Key,
  data: openArray[byte]): ?!void {.locks: "unknown".} =

  self.put(key, data, timestamp())

# method query*(
#   self: SQLiteDatastore,
#   query: ...): ?!(?...) {.locks: "unknown".} =
#
#   success ....none
