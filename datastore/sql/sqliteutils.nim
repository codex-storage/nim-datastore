import pkg/questionable
import pkg/questionable/results
import pkg/sqlite3_abi
import pkg/upraises

import ../backend

export sqlite3_abi

# Adapted from:
# https://github.com/status-im/nwaku/blob/master/waku/v2/node/storage/sqlite.nim

# see https://www.sqlite.org/c3ref/column_database_name.html
# can pass `--forceBuild:on` to the Nim compiler if a SQLite build without
# `-DSQLITE_ENABLE_COLUMN_METADATA` option is stuck in the build cache,
# e.g. `nimble test --forceBuild:on`
{.passc: "-DSQLITE_ENABLE_COLUMN_METADATA".}

push: {.upraises: [].}

type
  AutoDisposed*[T: ptr|ref] = object
    val*: T

  DataProc* = proc(s: RawStmtPtr) {.closure, gcsafe.}

  NoParams* = tuple # empty tuple

  NoParamsStmt* = SQLiteStmt[NoParams, void]

  RawStmtPtr* = ptr sqlite3_stmt

  SQLite* = ptr sqlite3

  SQLiteStmt*[Params, Res] = distinct RawStmtPtr

  # see https://github.com/arnetheduck/nim-sqlite3-abi/issues/4
  sqlite3_destructor_type_gcsafe =
    proc (a1: pointer) {.cdecl, gcsafe, upraises: [].}

const
  SQLITE_TRANSIENT_GCSAFE* = cast[sqlite3_destructor_type_gcsafe](-1)

proc bindParam(
  s: RawStmtPtr,
  n: int,
  val: auto): cint =

  when val is openArray[byte]|seq[byte]|DataBuffer:
    if val.len > 0:
      # `SQLITE_TRANSIENT` "indicate[s] that the object is to be copied prior
      # to the return from sqlite3_bind_*(). The object and pointer to it
      # must remain valid until then. SQLite will then manage the lifetime of
      # its private copy."
      sqlite3_bind_blob(s, n.cint, unsafeAddr val[0], val.len.cint, SQLITE_TRANSIENT)
    else:
      sqlite3_bind_null(s, n.cint)
  elif val is int32:
    sqlite3_bind_int(s, n.cint, val)
  elif val is uint32 | int64:
    sqlite3_bind_int64(s, n.cint, val.int64)
  elif val is float32 | float64:
    sqlite3_bind_double(s, n.cint, val.float64)
  elif val is string:
    # `-1` implies string length is num bytes up to first null-terminator;
    # `SQLITE_TRANSIENT` "indicate[s] that the object is to be copied prior
    # to the return from sqlite3_bind_*(). The object and pointer to it must
    # remain valid until then. SQLite will then manage the lifetime of its
    # private copy."
    sqlite3_bind_text(s, n.cint, val.cstring, val.len().cint, SQLITE_TRANSIENT)
  elif val is KeyId:
    # same as previous
    sqlite3_bind_text(s, n.cint, val.toCString(), val.data.len().cint, SQLITE_TRANSIENT)
  else:
    {.fatal: "Please add support for the '" & $typeof(val) & "' type".}

template bindParams(
  s: RawStmtPtr,
  params: auto) =

  when params is tuple:
    when params isnot NoParams:
      var i = 1
      for param in fields(params):
        checkErr bindParam(s, i, param)
        inc i

  else:
    checkErr bindParam(s, 1, params)

template checkErr*(op: untyped) =
  if (let v = (op); v != SQLITE_OK):
    return failure $sqlite3_errstr(v)

template dispose*(rawStmt: RawStmtPtr) =
  discard sqlite3_finalize(rawStmt)

template checkExec*(s: RawStmtPtr) =
  if (let x = sqlite3_step(s); x != SQLITE_DONE):
    s.dispose
    return failure $sqlite3_errstr(x)

  if (let x = sqlite3_finalize(s); x != SQLITE_OK):
    return failure $sqlite3_errstr(x)

template prepare*(
  env: SQLite,
  q: string,
  prepFlags: cuint = 0): RawStmtPtr =

  var
    s: RawStmtPtr

  checkErr sqlite3_prepare_v3(
    env, q.cstring, q.len.cint, prepFlags, addr s, nil)

  s

template checkExec*(env: SQLite, q: string) =
  let
    s = prepare(env, q)

  checkExec(s)

template dispose*(db: SQLite) =
  discard sqlite3_close(db)

template dispose*(sqliteStmt: SQLiteStmt) =
  discard sqlite3_finalize(RawStmtPtr(sqliteStmt))

proc release*[T](x: var AutoDisposed[T]): T =
  result = x.val
  x.val = nil

proc disposeIfUnreleased*[T](x: var AutoDisposed[T]) =
  mixin dispose
  if x.val != nil: dispose(x.release)

proc exec*[P](s: SQLiteStmt[P, void], params: P = ()): ?!void =

  let
    s = RawStmtPtr(s)

  bindParams(s, params)

  let
    res =
      if (let v = sqlite3_step(s); v != SQLITE_DONE):
        failure $sqlite3_errstr(v)
      else:
        success()

  # release implicit transaction
  discard sqlite3_reset(s) # same return information as step
  discard sqlite3_clear_bindings(s) # no errors possible

  res

proc sqlite3_column_text_not_null*(
  s: RawStmtPtr,
  index: cint): cstring =

  let
    text = sqlite3_column_text(s, index).cstring

  if text.isNil:
    # see the conversion table and final paragraph of:
    # https://www.sqlite.org/c3ref/column_blob.html
    # a null pointer here implies an out-of-memory error
    let
      v = sqlite3_errcode(sqlite3_db_handle(s))

    raise (ref Defect)(msg: $sqlite3_errstr(v))

  text

template journalModePragmaStmt*(env: SQLite): RawStmtPtr =
  let
    s = prepare(env, "PRAGMA journal_mode = WAL;")

  if (let x = sqlite3_step(s); x != SQLITE_ROW):
    s.dispose
    return failure $sqlite3_errstr(x)

  if (let x = sqlite3_column_type(s, 0); x != SQLITE3_TEXT):
    s.dispose
    return failure $sqlite3_errstr(x)

  let
    x = $sqlite3_column_text_not_null(s, 0)

  if not (x in ["memory", "wal"]):
    s.dispose
    return failure "Invalid pragma result: \"" & x & "\""

  s

template open*(
  dbPath: string,
  env: var SQLite,
  flags = 0) =

  checkErr sqlite3_open_v2(dbPath.cstring, addr env, flags.cint, nil)

proc prepare*[Params, Res](
  T: type SQLiteStmt[Params, Res],
  env: SQLite,
  stmt: string,
  prepFlags: cuint = 0): ?!T =

  var
    s: RawStmtPtr

  checkErr sqlite3_prepare_v3(
    env, stmt.cstring, stmt.len.cint, prepFlags, addr s, nil)

  success T(s)

proc query*[P](
  s: SQLiteStmt[P, void],
  params: P,
  onData: DataProc): ?!bool =

  let
    s = RawStmtPtr(s)

  bindParams(s, params)

  var
    res = success false

  while true:
    let
      v = sqlite3_step(s)

    case v
    of SQLITE_ROW:
      onData(s)
      res = success true
    of SQLITE_DONE:
      break
    else:
      res = failure $sqlite3_errstr(v)
      break

  # release implict transaction
  discard sqlite3_reset(s) # same return information as step
  discard sqlite3_clear_bindings(s) # no errors possible

  res

proc query*(
  env: SQLite,
  query: string,
  onData: DataProc): ?!bool =

  let
    s = ? NoParamsStmt.prepare(env, query)
    res = s.query((), onData)

  # NB: dispose of the prepared query statement and free associated memory
  s.dispose

  res
