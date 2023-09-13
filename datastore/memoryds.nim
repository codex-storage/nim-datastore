import std/tables
import std/sharedtables
import std/sharedlist
import std/sequtils
import std/strutils
import std/algorithm
import std/locks

import std/atomics

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/upraises

import ./key
import ./query
import ./datastore

export key, query

push: {.upraises: [].}

type
  MemoryDatastore* = ref object of Datastore
    store*: SharedTable[Key, seq[byte]]
    keys*: SharedList[Key]
    lock: Lock # yes, we need the lock since we need to update both the table and the list :facepalm:

template withLock(lock: Lock, body: untyped) =
  try:
    lock.acquire()
    body
  finally:
    lock.release()

method has*(
  self: MemoryDatastore,
  key: Key): Future[?!bool] {.async.} =
  let
    keys = toSeq(self.keys)

  for k in keys:
    if k == key:
      return success true

  return success false

method delete*(
  self: MemoryDatastore,
  key: Key): Future[?!void] {.async.} =

  withLock(self.lock):
    self.keys.iterAndMutate(proc(k: Key): bool = k == key)
    self.store.del(key)

  return success()

method delete*(
  self: MemoryDatastore,
  keys: seq[Key]): Future[?!void] {.async.} =

  for key in keys:
    if err =? (await self.delete(key)).errorOption:
      return failure err

  return success()

method get*(
  self: MemoryDatastore,
  key: Key): Future[?!seq[byte]] {.async.} =

  withLock(self.lock):
    let
      has = (await self.has(key))

    if has.isOk and has.get:
      return self.store.mget(key).catch()

  return failure (ref DatastoreError)(msg: "not found")

method put*(
  self: MemoryDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async.} =

  withLock(self.lock):
    if not self.store.hasKeyOrPut(key, data):
      self.keys.add(key)
    else:
      self.store[key] = data

method put*(
  self: MemoryDatastore,
  batch: seq[BatchEntry]): Future[?!void] {.async.} =

  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err

  return success()

method query*(
  self: MemoryDatastore,
  query: Query,
): Future[?!QueryIter] {.async.} =

  let
    queryKey = query.key.id()
    keys = toSeq(self.keys)

  var
    iter = QueryIter.new()
    pos = 0

  proc next(): Future[?!QueryResponse] {.async.} =
    defer:
      pos.inc

    if iter.finished:
      return failure (ref QueryEndedError)(msg: "Calling next on a finished query!")

    if pos > keys.len - 1:
      iter.finished = true
      return success (Key.none, EmptyBytes)

    return success (
      Key.init(keys[pos]).expect("Should not fail!").some,
      if query.value: self.store.mget(keys[pos]) else: EmptyBytes)

  iter.next = next
  return success iter

method close*(self: MemoryDatastore): Future[?!void] {.async.} =
  self.store.deinitSharedTable()
  self.keys.deinitSharedList()
  self.lock.deinitLock()
  return success()

proc new*(tp: type MemoryDatastore): MemoryDatastore =
  var
    table: SharedTable[Key, seq[byte]]
    keys: SharedList[Key]
    lock: Lock

  table.init()
  keys.init()
  lock.initLock()
  MemoryDatastore(store: table, keys: keys, lock: lock)
