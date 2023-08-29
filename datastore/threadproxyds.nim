import std/tables

import pkg/chronos
import pkg/chronos/threadsync
import pkg/questionable
import pkg/questionable/results
import pkg/upraises
import pkg/taskpools
import pkg/stew/results
import pkg/threading/smartptrs

import ./key
import ./query
import ./datastore
import ./threadbackend
import ./fsds

import pretty

export key, query

push: {.upraises: [].}

type
  ThreadProxyDatastore* = ref object of Datastore
    tds: ThreadDatastorePtr

method has*(
  self: ThreadProxyDatastore,
  key: Key
): Future[?!bool] {.async.} =

  without ret =? newThreadResult(bool), err:
    return failure(err)

  try:
    has(ret, self.tds, key)
    await wait(ret[].signal)
  finally:
    echo "closing signal"
    ret[].signal.close()

  echo "\nSharedDataStore:has:value: ", ret[].repr
  return success(ret[].value)

method delete*(
  self: ThreadProxyDatastore,
  key: Key
): Future[?!void] {.async.} =

  without ret =? newThreadResult(void), err:
    return failure(err)

  try:
    delete(ret, self.tds, key)
    await wait(ret[].signal)
  finally:
    echo "closing signal"
    ret[].signal.close()

  echo "\nSharedDataStore:put:value: ", ret[].repr
  return success()

method delete*(
  self: ThreadProxyDatastore,
  keys: seq[Key]
): Future[?!void] {.async.} =

  for key in keys:
    if err =? (await self.delete(key)).errorOption:
      return failure err

  return success()

method get*(
  self: ThreadProxyDatastore,
  key: Key
): Future[?!seq[byte]] {.async.} =

  without ret =? newThreadResult(DataBuffer), err:
    return failure(err)

  try:
    get(ret, self.tds, key)
    await wait(ret[].signal)
  finally:
    echo "closing signal"
    ret[].signal.close()

  print "\nSharedDataStore:put:value: ", ret[]
  let data = ret[].value.toSeq(byte)
  return success(data)

method put*(
  self: ThreadProxyDatastore,
  key: Key,
  data: seq[byte]
): Future[?!void] {.async.} =

  without ret =? newThreadResult(void), err:
    return failure(err)

  echo "res: ", ret
  try:
    put(ret, self.tds, key, data)
    await wait(ret[].signal)
  finally:
    echo "closing signal"
    ret[].signal.close()

  echo "\nSharedDataStore:put:value: ", ret[].repr
  return success()

method put*(
  self: ThreadProxyDatastore,
  batch: seq[BatchEntry]
): Future[?!void] {.async.} =

  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err

  return success()

method close*(
  self: ThreadProxyDatastore
): Future[?!void] {.async.} =
  # TODO: how to handle failed close?
  echo "ThreadDatastore: FREE: "
  result = success()

  without res =? self.tds[].ds.close(), err:
    result = failure(err)
  # GC_unref(self.tds[].ds) ## TODO: is this needed?

  if self.tds[].tp != nil:
    ## this can block... how to handle? maybe just leak?
    self.tds[].tp.shutdown()

proc newSharedDataStore*(
  ds: Datastore,
): ?!ThreadProxyDatastore =

  var self = ThreadProxyDatastore()

  let value = newSharedPtr(ThreadDatastore)
  echo "\nnewDataStore: threadId:", getThreadId()
  # GC_ref(ds)
  value[].ds = ds
  try:
    value[].tp = Taskpool.new(num_threads = 2)
  except Exception as exc:
    return err((ref DatastoreError)(msg: exc.msg))

  self.tds = value

  success self
