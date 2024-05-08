import std/os
import std/strformat

import pkg/questionable
import pkg/questionable/results
import pkg/upraises

import ./sqliteutils

type
  BoundIdCol* = proc (): string {.closure, gcsafe, upraises: [].}
  BoundVersionCol* = proc (): int64 {.closure, gcsafe, upraises: [].}
  BoundDataCol* = proc (): seq[byte] {.closure, gcsafe, upraises: [].}
  BoundTimestampCol* = proc (): int64 {.closure, gcsafe, upraises: [].}

  # feels odd to use `void` for prepared statements corresponding to SELECT
  # queries but it fits with the rest of the SQLite wrapper adapted from
  # status-im/nwaku, at least in its current form in ./sqlite
  ContainsStmt* = SQLiteStmt[(string), void]
  DeleteStmt* = SQLiteStmt[(string), void]
  GetStmt* = SQLiteStmt[(string), void]
  PutStmt* = SQLiteStmt[(string, seq[byte], int64, int64), void]
  QueryStmt* = SQLiteStmt[(string), void]
  GetVersionedStmt* = SQLiteStmt[(string), void]
  InsertVersionedStmt* = SQLiteStmt[(string, seq[byte], int64, int64), void]
  UpdateVersionedStmt* = SQLiteStmt[(seq[byte], int64, int64, string, int64), void]
  DeleteVersionedStmt* = SQLiteStmt[(string, int64), void]
  GetChangesStmt* = NoParamsStmt
  BeginStmt* = NoParamsStmt
  EndStmt* = NoParamsStmt
  RollbackStmt* = NoParamsStmt

  SQLiteDsDb* = object
    readOnly*: bool
    dbPath*: string
    containsStmt*: ContainsStmt
    deleteStmt*: DeleteStmt
    env*: SQLite
    getDataCol*: BoundDataCol
    getStmt*: GetStmt
    putStmt*: PutStmt
    getVersionedStmt*: GetVersionedStmt
    updateVersionedStmt*: UpdateVersionedStmt
    insertVersionedStmt*: InsertVersionedStmt
    deleteVersionedStmt*: DeleteVersionedStmt
    getChangesStmt*: GetChangesStmt
    beginStmt*: BeginStmt
    endStmt*: EndStmt
    rollbackStmt*: RollbackStmt

const
  DbExt* = ".sqlite3"
  TableName* = "Store"

  IdColName* = "id"
  DataColName* = "data"
  VersionColName* = "version"
  TimestampColName* = "timestamp"

  IdColType = "TEXT"
  DataColType = "BLOB"
  VersionColType = "INTEGER"
  TimestampColType = "INTEGER"

  Memory* = ":memory:"

  # https://stackoverflow.com/a/9756276
  # EXISTS returns a boolean value represented by an integer:
  # https://sqlite.org/datatype3.html#boolean_datatype
  # https://sqlite.org/lang_expr.html#the_exists_operator
  ContainsStmtStr* = """
    SELECT EXISTS(
      SELECT 1 FROM """ & TableName & """
      WHERE """ & IdColName & """ = ?
    )
  """

  ContainsStmtExistsCol* = 0

  CreateStmtStr* = """
    CREATE TABLE IF NOT EXISTS """ & TableName & """ (
      """ & IdColName & """ """ & IdColType & """ NOT NULL PRIMARY KEY,
      """ & DataColName & """ """ & DataColType & """,
      """ & VersionColName & """ """ & VersionColType & """ NOT NULL,
      """ & TimestampColName & """ """ & TimestampColType & """ NOT NULL
    ) WITHOUT ROWID;
  """

  DeleteStmtStr* = """
    DELETE FROM """ & TableName & """
    WHERE """ & IdColName & """ = ?
  """

  GetStmtStr* = """
    SELECT """ & DataColName & """ FROM """ & TableName & """
    WHERE """ & IdColName & """ = ?
  """

  GetStmtDataCol* = 0

  PutStmtStr* = """
    REPLACE INTO """ & TableName & """ (
      """ & IdColName & """,
      """ & DataColName & """,
      """ & VersionColName & """,
      """ & TimestampColName & """
    ) VALUES (?, ?, ?, ?)
  """

  QueryStmtIdStr* = """
    SELECT """ & IdColName & """ FROM """ & TableName &
        """ WHERE """ & IdColName & """ GLOB ?
  """

  QueryStmtDataIdStr* = """
    SELECT """ & IdColName & """, """ & DataColName & """ FROM """ & TableName &
        """ WHERE """ & IdColName & """ GLOB ?
  """

  QueryStmtOffset* = """
    OFFSET ?
  """

  QueryStmtLimit* = """
    LIMIT ?
  """

  QueryStmtOrderAscending* = """
    ORDER BY """ & IdColName & """ ASC
  """

  QueryStmtOrderDescending* = """
    ORDER BY """ & IdColName & """ DESC
  """

  GetVersionedStmtStr* = fmt"""
    SELECT {DataColName}, {VersionColName} FROM {TableName}
    WHERE {IdColName} = ?
  """

  GetVersionedStmtDataCol* = 0
  GetVersionedStmtVersionCol* = 1

  InsertVersionedStmtStr* = fmt"""
    INSERT INTO {TableName}
    (
      {IdColName},
      {DataColName},
      {VersionColName},
      {TimestampColName}
    )
    VALUES (?, ?, ?, ?)
  """

  UpdateVersionedStmtStr* = fmt"""
    UPDATE {TableName}
    SET
      {DataColName} = ?,
      {VersionColName} = ?,
      {TimestampColName} = ?
    WHERE {IdColName} = ? AND {VersionColName} = ?
  """

  DeleteVersionedStmtStr* = fmt"""
    DELETE FROM {TableName}
    WHERE {IdColName} = ? AND {VersionColName} = ?
  """

  GetChangesStmtStr* = fmt"""
    SELECT changes()
  """

  BeginTransactionStr* = """
    BEGIN;
  """

  EndTransactionStr* = """
    END;
  """

  RollbackTransactionStr* = """
    ROLLBACK;
  """

  QueryStmtIdCol* = 0
  QueryStmtDataCol* = 1

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

  checkColMetadata(s, index, IdColName)

  return proc (): string =
    $sqlite3_column_text_not_null(s, index.cint)

proc dataCol*(
  s: RawStmtPtr,
  index: int): BoundDataCol =

  checkColMetadata(s, index, DataColName)

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

  checkColMetadata(s, index, TimestampColName)

  return proc (): int64 =
    sqlite3_column_int64(s, index.cint)

proc versionCol*(
  s: RawStmtPtr,
  index: int): BoundVersionCol =

  checkColMetadata(s, index, VersionColName)

  return proc (): int64 =
    sqlite3_column_int64(s, index.cint)

proc changesCol*(
  s: RawStmtPtr,
  index: int): BoundVersionCol =
  return proc (): int64 =
    sqlite3_column_int64(s, index.cint)

proc getDBFilePath*(path: string): ?!string =
  try:
    let
      (parent, name, ext) = path.normalizePathEnd.splitFile
      dbExt = if ext == "": DbExt else: ext
      absPath =
        if parent.isAbsolute: parent
        else: getCurrentDir() / parent
      dbPath = absPath / name & dbExt

    return success dbPath
  except CatchableError as exc:
    return failure(exc.msg)

proc close*(self: var SQLiteDsDb) =

  var
    env: AutoDisposed[SQLite]

  defer:
    disposeIfUnreleased(env)

  env.val = self.env

  if not RawStmtPtr(self.containsStmt).isNil:
    self.containsStmt.dispose

  if not RawStmtPtr(self.getStmt).isNil:
    self.getStmt.dispose

  if not RawStmtPtr(self.beginStmt).isNil:
    self.beginStmt.dispose

  if not RawStmtPtr(self.endStmt).isNil:
    self.endStmt.dispose

  if not RawStmtPtr(self.rollbackStmt).isNil:
    self.rollbackStmt.dispose

  if not RawStmtPtr(self.deleteStmt).isNil:
    self.deleteStmt.dispose

  if not RawStmtPtr(self.putStmt).isNil:
    self.putStmt.dispose

  if not RawStmtPtr(self.getVersionedStmt).isNil:
    self.getVersionedStmt.dispose

  if not RawStmtPtr(self.updateVersionedStmt).isNil:
    self.updateVersionedStmt.dispose

  if not RawStmtPtr(self.insertVersionedStmt).isNil:
    self.insertVersionedStmt.dispose

  if not RawStmtPtr(self.deleteVersionedStmt).isNil:
    self.deleteVersionedStmt.dispose

  if not RawStmtPtr(self.getChangesStmt).isNil:
    self.getChangesStmt.dispose

proc open*(
  T: type SQLiteDsDb,
  path = Memory,
  flags = SQLITE_OPEN_READONLY): ?!SQLiteDsDb =

  # make it optional to enable WAL with it enabled being the default?

  # make it possible to specify a custom page size?
  # https://www.sqlite.org/pragma.html#pragma_page_size
  # https://www.sqlite.org/intern-v-extern-blob.html

  var
    env: AutoDisposed[SQLite]

  defer:
    disposeIfUnreleased(env)

  let
    isMemory = path == Memory
    absPath = if isMemory: Memory else: ?path.getDBFilePath
    readOnly = (SQLITE_OPEN_READONLY and flags).bool

  if not isMemory:
    if readOnly and not fileExists(absPath):
      return failure "read-only database does not exist: " & absPath
    elif not dirExists(absPath.parentDir):
      return failure "directory does not exist: " & absPath

  open(absPath, env.val, flags)

  var
    pragmaStmt = journalModePragmaStmt(env.val)

  checkExec(pragmaStmt)

  var
    containsStmt: ContainsStmt
    deleteStmt: DeleteStmt
    getStmt: GetStmt
    putStmt: PutStmt
    getVersionedStmt: GetVersionedStmt
    updateVersionedStmt: UpdateVersionedStmt
    insertVersionedStmt: InsertVersionedStmt
    deleteVersionedStmt: DeleteVersionedStmt
    getChangesStmt: GetChangesStmt
    beginStmt: BeginStmt
    endStmt: EndStmt
    rollbackStmt: RollbackStmt

  if not readOnly:
    checkExec(env.val, CreateStmtStr)

    deleteStmt = ? DeleteStmt.prepare(
      env.val, DeleteStmtStr, SQLITE_PREPARE_PERSISTENT)

    putStmt = ? PutStmt.prepare(
      env.val, PutStmtStr, SQLITE_PREPARE_PERSISTENT)

    insertVersionedStmt = ? InsertVersionedStmt.prepare(
      env.val, InsertVersionedStmtStr, SQLITE_PREPARE_PERSISTENT)

    updateVersionedStmt = ? UpdateVersionedStmt.prepare(
      env.val, UpdateVersionedStmtStr, SQLITE_PREPARE_PERSISTENT)

    deleteVersionedStmt = ? DeleteVersionedStmt.prepare(
      env.val, DeleteVersionedStmtStr, SQLITE_PREPARE_PERSISTENT)

    getChangesStmt = ? GetChangesStmt.prepare(
      env.val, GetChangesStmtStr, SQLITE_PREPARE_PERSISTENT)

  beginStmt = ? BeginStmt.prepare(
    env.val, BeginTransactionStr, SQLITE_PREPARE_PERSISTENT)

  endStmt = ? EndStmt.prepare(
    env.val, EndTransactionStr, SQLITE_PREPARE_PERSISTENT)

  rollbackStmt = ? RollbackStmt.prepare(
    env.val, RollbackTransactionStr, SQLITE_PREPARE_PERSISTENT)

  containsStmt = ? ContainsStmt.prepare(
    env.val, ContainsStmtStr, SQLITE_PREPARE_PERSISTENT)

  getStmt = ? GetStmt.prepare(
    env.val, GetStmtStr, SQLITE_PREPARE_PERSISTENT)

  getVersionedStmt = ? GetVersionedStmt.prepare(
    env.val, GetVersionedStmtStr, SQLITE_PREPARE_PERSISTENT)

  # if a readOnly/existing database does not satisfy the expected schema
  # `pepare()` will fail and `new` will return an error with message
  # "SQL logic error"

  let
    getDataCol = dataCol(RawStmtPtr(getStmt), GetStmtDataCol)

  success T(
    readOnly: readOnly,
    dbPath: path,
    containsStmt: containsStmt,
    deleteStmt: deleteStmt,
    env: env.release,
    getStmt: getStmt,
    getDataCol: getDataCol,
    putStmt: putStmt,
    getVersionedStmt: getVersionedStmt,
    updateVersionedStmt: updateVersionedStmt,
    insertVersionedStmt: insertVersionedStmt,
    deleteVersionedStmt: deleteVersionedStmt,
    getChangesStmt: getChangesStmt,
    beginStmt: beginStmt,
    endStmt: endStmt,
    rollbackStmt: rollbackStmt)
