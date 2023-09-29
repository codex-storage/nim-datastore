import std/os
import std/options
import std/strutils

import pkg/questionable
import pkg/questionable/results
from pkg/stew/results as stewResults import get, isErr
import pkg/upraises
import pkg/chronos
import pkg/taskpools

import ./threads/sqlbackend
import ./threads/threadproxyds
import ./datastore

export datastore, Taskpool

push: {.upraises: [].}


type
  FSDatastore* = ref object of Datastore
    db: ThreadDatastore[SQLiteBackend[KeyId, DataBuffer]]

proc path*(self: FSDatastore): string =
  self.db.backend.path()

proc readOnly*(self: FSDatastore): bool =
  self.db.backend.readOnly()

method has*(self: FSDatastore,
            key: Key): Future[?!bool] {.async.} =
  await self.db.has(key)

method delete*(self: FSDatastore,
               key: Key): Future[?!void] {.async.} =
  await self.db.delete(key)

method delete*(self: FSDatastore,
               keys: seq[Key]): Future[?!void] {.async.} =
  await self.db.delete(keys)

method get*(self: FSDatastore,
            key: Key): Future[?!seq[byte]] {.async.} =
  await self.db.get(key)

method put*(self: FSDatastore,
            key: Key,
            data: seq[byte]): Future[?!void] {.async.} =
  await self.db.put(key, data)

method put*(self: FSDatastore,
            batch: seq[BatchEntry]): Future[?!void] {.async.} =
  await self.db.put(batch)

method close*(self: FSDatastore): Future[?!void] {.async.} =
  await self.db.close()

method query*(self: FSDatastore,
              q: Query): Future[?!QueryIter] {.async.} =
  await self.db.query(q)

proc new*(
  T: type FSDatastore,
  path: string,
  readOnly = false,
  tp: Taskpool,
): ?!FSDatastore =

  let
    backend = ? newSQLiteBackend[KeyId, DataBuffer](path, readOnly)
    db = ? ThreadDatastore.new(backend, tp = tp)
  success FSDatastore(db: db)
