import std/os

import pkg/chronos
import pkg/asynctest
import pkg/stew/byteutils

import pkg/sqlite3_abi
import pkg/datastore/key
import pkg/datastore/sql/sqlitedsdb
import pkg/datastore/sql/sqliteutils
import pkg/datastore/sql/sqliteds

suite "Test Open SQLite Datastore DB":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    filename = "test_store" & DbExt
    dbPathAbs = basePathAbs / filename

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

  teardownAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  test "Should create and open datastore DB":
    var
      dsDb = SQLiteDsDb.open(
        path = dbPathAbs,
        flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE).tryGet()

    defer:
      dsDb.close()

      check:
        fileExists(dbPathAbs)

  test "Should open existing DB":
    var
      dsDb = SQLiteDsDb.open(
        path = dbPathAbs,
        flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE).tryGet()

    defer:
      dsDb.close()

      check:
        fileExists(dbPathAbs)

  test "Should open existing DB in read only mode":
    check:
      fileExists(dbPathAbs)

    var
      dsDb = SQLiteDsDb.open(
        path = dbPathAbs,
        flags = SQLITE_OPEN_READONLY).tryGet()

    defer:
      dsDb.close()

  test "Should fail open non existent DB in read only mode":
    removeDir(basePathAbs)
    check:
      not fileExists(dbPathAbs)
      SQLiteDsDb.open(path = dbPathAbs).isErr

suite "Test SQLite Datastore DB operations":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    filename = "test_store" & DbExt
    dbPathAbs = basePathAbs / filename

    key = Key.init("test/key").tryGet()
    data = "some data".toBytes
    otherData = "some other data".toBytes

  var
    dsDb: SQLiteDsDb
    readOnlyDb: SQLiteDsDb

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    dsDb = SQLiteDsDb.open(
      path = dbPathAbs,
      flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE).tryGet()

    readOnlyDb = SQLiteDsDb.open(
      path = dbPathAbs,
      flags = SQLITE_OPEN_READONLY).tryGet()

  teardownAll:
    dsDb.close()
    readOnlyDb.close()

    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  test "Should insert key":
    check:
      readOnlyDb.putStmt.exec((key.id, data, initVersion, timestamp())).isErr()

    dsDb.putStmt.exec((key.id, data, initVersion, timestamp())).tryGet()

  test "Should select key":
    let
      dataCol = dsDb.getDataCol

    var bytes: seq[byte]
    proc onData(s: RawStmtPtr) =
      bytes = dataCol()

    check:
      dsDb.getStmt.query((key.id), onData).tryGet()
      bytes == data

  test "Should update key":
    check:
      readOnlyDb.putStmt.exec((key.id, otherData, initVersion, timestamp())).isErr()

    dsDb.putStmt.exec((key.id, otherData, initVersion, timestamp())).tryGet()

  test "Should select updated key":
    let
      dataCol = dsDb.getDataCol

    var bytes: seq[byte]
    proc onData(s: RawStmtPtr) =
      bytes = dataCol()

    check:
      dsDb.getStmt.query((key.id), onData).tryGet()
      bytes == otherData

  test "Should delete key":
    check:
      readOnlyDb.deleteStmt.exec((key.id)).isErr()

    dsDb.deleteStmt.exec((key.id)).tryGet()

  test "Should not contain key":
    var
      exists = false

    proc onData(s: RawStmtPtr) =
      exists = sqlite3_column_int64(s, ContainsStmtExistsCol.cint).bool

    check:
      dsDb.containsStmt.query((key.id), onData).tryGet()
      not exists
