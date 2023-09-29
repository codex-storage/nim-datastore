
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
import ./threads/threadproxy
import ./datastore

export datastore, keys, query, Taskpool, Memory, DbExt

push: {.upraises: [].}

type
  SQLiteDatastore* = ref object of Datastore
    db: ThreadProxy[SQLiteBackend[KeyId, DataBuffer]]

proc path*(self: SQLiteDatastore): string =
  self.db.backend.path()

proc readOnly*(self: SQLiteDatastore): bool =
  self.db.backend.readOnly()

method has*(self: SQLiteDatastore,
            key: Key): Future[?!bool] {.async.} =
  await self.db.has(key)

method delete*(self: SQLiteDatastore,
               key: Key): Future[?!void] {.async.} =
  await self.db.delete(key)

method delete*(self: SQLiteDatastore,
               keys: seq[Key]): Future[?!void] {.async.} =
  await self.db.delete(keys)

method get*(self: SQLiteDatastore,
            key: Key): Future[?!seq[byte]] {.async.} =
  await self.db.get(key)

method put*(self: SQLiteDatastore,
            key: Key,
            data: seq[byte]): Future[?!void] {.async.} =
  await self.db.put(key, data)

method put*(self: SQLiteDatastore,
            batch: seq[BatchEntry]): Future[?!void] {.async.} =
  await self.db.put(batch)

method close*(self: SQLiteDatastore): Future[?!void] {.async.} =
  await self.db.close()

method query*(self: SQLiteDatastore,
              q: Query): Future[?!QueryIter] {.async.} =
  await self.db.query(q)

proc new*(
  T: type SQLiteDatastore,
  path: string,
  tp: Taskpool = Taskpool.new(4),
  readOnly = false): ?!SQLiteDatastore =

  let
    backend = ? newSQLiteBackend[KeyId, DataBuffer](path, readOnly)
    db = ? ThreadProxy.new(backend, tp = tp)
  success SQLiteDatastore(db: db)
