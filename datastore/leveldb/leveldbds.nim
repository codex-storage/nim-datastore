{.push raises: [].}

import std/options
import std/tables
import std/os
import std/strformat
import std/strutils

import pkg/leveldbstatic
import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/stew/byteutils

import ../datastore
import ../defaultimpl

type
  LevelDbDatastore* = ref object of Datastore
    db: LevelDb
    locks: TableRef[Key, AsyncLock]

method has*(self: LevelDbDatastore, key: Key): Future[?!bool] {.async.} =
  try:
    let str = self.db.get($key)
    return success(str.isSome)
  except LevelDbException as e:
    return failure("LevelDbDatastore.has exception: " & e.msg)

method delete*(self: LevelDbDatastore, key: Key): Future[?!void] {.async.} =
  try:
    self.db.delete($key, sync = true)
    return success()
  except LevelDbException as e:
    return failure("LevelDbDatastore.delete exception: " & e.msg)

method delete*(self: LevelDbDatastore, keys: seq[Key]): Future[?!void] {.async.} =
  for key in keys:
    if err =? (await self.delete(key)).errorOption:
      return failure(err.msg)
  return success()

method get*(self: LevelDbDatastore, key: Key): Future[?!seq[byte]] {.async.} =
  try:
    let str = self.db.get($key)
    if not str.isSome:
      return failure(newException(DatastoreKeyNotFound, "LevelDbDatastore.get: key not found " & $key))
    let bytes = str.get().toBytes()
    return success(bytes)
  except LevelDbException as e:
    return failure("LevelDbDatastore.get exception: " & $e.msg)

method put*(self: LevelDbDatastore, key: Key, data: seq[byte]): Future[?!void] {.async.} =
  try:
    let str = string.fromBytes(data)
    self.db.put($key, str)
    return success()
  except LevelDbException as e:
    return failure("LevelDbDatastore.put exception: " & $e.msg)

method put*(self: LevelDbDatastore, batch: seq[BatchEntry]): Future[?!void] {.async.} =
  try:
    let b = newBatch()
    for entry in batch:
      b.put($(entry.key), string.fromBytes(entry.data))
    self.db.write(b)
    return success()
  except LevelDbException as e:
    return failure("LevelDbDatastore.put (batch) exception: " & $e.msg)

method close*(self: LevelDbDatastore): Future[?!void] {.async.} =
  try:
    self.db.close()
    return success()
  except LevelDbException as e:
    return failure("LevelDbDatastore.close exception: " & $e.msg)

proc getQueryString(query: Query): string =
  result = $(query.key)
  let toTrim = ["/*", "\\*"]
  for trim in toTrim:
    if result.endsWith(trim):
      result = result[0 ..< ^(trim.len)]

method query*(
  self: LevelDbDatastore,
  query: Query): Future[?!QueryIter] {.async, gcsafe.} =

  if not (query.sort == SortOrder.Assending):
    return failure("LevelDbDatastore.query: query.sort is not SortOrder.Ascending. Unsupported.")

  var
    iter = QueryIter()
    dbIter = self.db.queryIter(
      prefix = getQueryString(query),
      keysOnly = not query.value,
      skip = query.offset,
      limit = query.limit
    )

  proc next(): Future[?!QueryResponse] {.async.} =
    if iter.finished:
      return failure(newException(QueryEndedError, "Calling next on a finished query!"))

    try:
      let (keyStr, valueStr) = dbIter.next()

      if dbIter.finished:
        iter.finished = true
        return success (Key.none, EmptyBytes)
      else:
        let key = Key.init(keyStr).expect("LevelDbDatastore.query (next) Failed to create key.")
        return success (key.some, valueStr.toBytes())
    except LevelDbException as e:
      return failure("LevelDbDatastore.query -> next exception: " & $e.msg)

  proc dispose(): Future[?!void] {.async.} =
    dbIter.dispose()
    return success()

  iter.next = next
  iter.dispose = dispose
  return success iter

method modifyGet*(
  self: LevelDbDatastore,
  key: Key,
  fn: ModifyGet): Future[?!seq[byte]] {.async.} =
  var lock: AsyncLock
  try:
    lock = self.locks.mgetOrPut(key, newAsyncLock())
    return await defaultModifyGetImpl(self, lock, key, fn)
  finally:
    if not lock.locked:
      self.locks.del(key)

method modify*(
  self: LevelDbDatastore,
  key: Key,
  fn: Modify): Future[?!void] {.async.} =
  var lock: AsyncLock
  try:
    lock = self.locks.mgetOrPut(key, newAsyncLock())
    return await defaultModifyImpl(self, lock, key, fn)
  finally:
    if not lock.locked:
      self.locks.del(key)

proc new*(
  T: type LevelDbDatastore, dbName: string): ?!T =
  try:
    let db = leveldbstatic.open(dbName)

    success T(
      db: db,
      locks: newTable[Key, AsyncLock]()
    )
  except LevelDbException as e:
    return failure("LevelDbDatastore.new exception: " & $e.msg)
