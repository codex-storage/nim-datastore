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
import ./threads/databuffer
import ./threads/simpletable

import std/locks

export key, query

push: {.upraises: [].}

type
  MemoryDatastore* = object of Datastore2
    lock*: Lock
    store*: SimpleTable[10_000]

proc has*(
    self: var MemoryDatastore,
    key: KeyBuffer
): ?!bool =

  withLock(self.lock):
    let res: bool = self.store.hasKey(key)
    return success res

proc delete*(
    self: var MemoryDatastore,
    key: KeyBuffer
): ?!void =

  var val: ValueBuffer
  withLock(self.lock):
    discard self.store.pop(key, val)
  return success()

proc get*(
    self: var MemoryDatastore,
    key: KeyBuffer
): ?!ValueBuffer =

  let dk = key
  withLock(self.lock):
    let res = self.store[dk].catch
    return res

proc put*(
    self: var MemoryDatastore,
    key: KeyBuffer,
    data: ValueBuffer
): Future[?!void] {.async.} =

  withLock(self.lock):
    self.store[key] = data
  return success()

proc close*(self: var MemoryDatastore): ?!void =
  self.store.clear()
  return success()

func new*(tp: typedesc[MemoryDatastore]): MemoryDatastore =
  var self = tp()
  self.lock.initLock()
  self.has = has
  self.delete = delete
  self.get = get
  self.put = put
  self.close = close
  return self
