import std/tables
import std/sequtils
import std/strutils
import std/algorithm

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/upraises

import ./key
import ./query
import ./datastore2
import ./threads/sharedptr
import ./threads/databuffer
import ./threads/simpletable

import std/locks

export key, query, datastore2

push: {.upraises: [].}

type
  MemoryDatastore* = object
    lock*: Lock
    store*: SimpleTable[10_000]

proc has(self: SharedPtr[MemoryDatastore], key: KeyBuffer): ?!bool =

  withLock(self[].lock):
    let res: bool = self[].store.hasKey(key)
    return success res

proc delete(
    self: SharedPtr[MemoryDatastore],
    key: KeyBuffer
): ?!void =

  var val: ValueBuffer
  withLock(self[].lock):
    discard self[].store.pop(key, val)
  return success()

proc get(
    self: SharedPtr[MemoryDatastore],
    key: KeyBuffer
): ?!ValueBuffer =

  let dk = key
  withLock(self[].lock):
    let res = self[].store[dk].catch
    return res

proc put(
    self: SharedPtr[MemoryDatastore],
    key: KeyBuffer,
    data: ValueBuffer
): ?!void =

  withLock(self[].lock):
    self[].store[key] = data
  return success()

proc close(self: SharedPtr[MemoryDatastore]): ?!void =
  self[].store.clear()
  return success()

proc initMemoryDatastore*(): Datastore2[MemoryDatastore] =
  var self = Datastore2[MemoryDatastore]()
  self.ids = newSharedPtr(MemoryDatastore)
  self.ids[].lock.initLock()
  self.has = has
  self.delete = delete
  self.get = get
  self.put = put
  self.close = close
  return self
