import std/tables

import pkg/chronos
import pkg/chronos/threadsync
import pkg/questionable
import pkg/questionable/results
import pkg/upraises
import pkg/taskpools
import pkg/stew/results

import ./key
import ./query
import ./datastore
import ./threadbackend
import threading/smartptrs

import pretty

export key, query, ThreadBackend

push: {.upraises: [].}

type

  SharedDatastore* = ref object of Datastore
    # stores*: Table[Key, SharedDatastore]
    tds: ThreadDatastorePtr

method has*(
  self: SharedDatastore,
  key: Key
): Future[?!bool] {.async.} =
  return success(true)

method delete*(
  self: SharedDatastore,
  key: Key
): Future[?!void] {.async.} =
  return success()

method delete*(
  self: SharedDatastore,
  keys: seq[Key]
): Future[?!void] {.async.} =
  return success()

method get*(
  self: SharedDatastore,
  key: Key
): Future[?!seq[byte]] {.async.} =

  without ret =? newThreadResult(DataBuffer), err:
    return failure(err)

  get(ret, self.tds, key)
  await wait(ret[].signal)
  ret[].signal.close()

  echo "\nSharedDataStore:put:value: ", ret[].repr
  let data = ret[].value.toSeq(byte)
  return success(data)

method put*(
  self: SharedDatastore,
  key: Key,
  data: seq[byte]
): Future[?!void] {.async.} =

  without ret =? newThreadResult(void), err:
    return failure(err)

  echo "res: ", ret
  put(ret, self.tds, key, data)
  await wait(ret[].signal)
  echo "closing signal"
  ret[].signal.close()

  echo "\nSharedDataStore:put:value: ", ret[].repr
  return success()


method put*(
  self: SharedDatastore,
  batch: seq[BatchEntry]
): Future[?!void] {.async.} =
  raiseAssert("Not implemented!")

method close*(
  self: SharedDatastore
): Future[?!void] {.async.} =

  # TODO: how to handle failed close?
  return success()

proc newSharedDataStore*(
  # T: typedesc[SharedDatastore],
  backend: ThreadBackend,
): Future[?!SharedDatastore] {.async.} =

  var
    self = SharedDatastore()

  without res =? newThreadResult(ThreadDatastorePtr), err:
    return failure(err)
  
  res[].value = newSharedPtr(ThreadDatastore)

  echo "\nnewDataStore: threadId:", getThreadId()
  res.createThreadDatastore(backend)
  await wait(res[].signal)
  res[].signal.close()

  echo "\nnewSharedDataStore:state: ", res[].state.repr
  echo "\nnewSharedDataStore:value: ", res[].value[].backend.repr

  self.tds = res[].value

  success self
