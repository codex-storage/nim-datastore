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
import ./threads/threadbackend

export key, query

push: {.upraises: [].}

type
  ThreadProxyDatastore* = ref object of Datastore
    tds: ThreadDatastorePtr

method has*(
  self: ThreadProxyDatastore,
  key: Key
): Future[?!bool] {.async.} =

  var ret = await newThreadResult(bool)

  try:
    has(ret, self.tds, key)
    await wait(ret)
    return ret.convert(bool)
  finally:
    ret.release()

method delete*(
  self: ThreadProxyDatastore,
  key: Key
): Future[?!void] {.async.} =

  var ret = await newThreadResult(void)

  try:
    delete(ret, self.tds, key)
    await wait(ret)
  finally:
    ret.release()

  return ret.convert(void)

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
  ## implements batch get
  ## 
  ## note: this implementation is rather naive and should
  ## probably be switched to use a single ThreadSignal
  ## for the entire batch

  var ret = await newThreadResult(ValueBuffer)

  try:
    get(ret, self.tds, key)
    await wait(ret)
  finally:
    ret.release()

  return ret.convert(seq[byte])

method put*(
  self: ThreadProxyDatastore,
  key: Key,
  data: seq[byte]
): Future[?!void] {.async.} =

  echo "put new request thr: ", $getThreadId()
  var ret = await newThreadResult(void)

  try:
    put(ret, self.tds, key, data)
    echo "\n"
    echo "wait put thr: ", $getThreadId()
    echo "\n"
    await sleepAsync(400.milliseconds)
    await wait(ret)
    echo "\n"
    await sleepAsync(400.milliseconds)

    return ret.convert(void)
  finally:
    echo "\n"
    await sleepAsync(400.milliseconds)
    echo "PUT RELEASE"
    ret.release()

method put*(
  self: ThreadProxyDatastore,
  batch: seq[BatchEntry]
): Future[?!void] {.async.} =
  ## implements batch put
  ## 
  ## note: this implementation is rather naive and should
  ## probably be switched to use a single ThreadSignal
  ## for the entire batch

  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err

  return success()

import pretty

method query*(
  self: ThreadProxyDatastore,
  query: Query
): Future[?!QueryIter] {.async.} =

  var ret = await newThreadResult(QueryResponseBuffer)

  # echo "\n\n=== Query Start === "

  ## we need to setup the query iter on the main thread
  ## to keep it's lifetime associated with this async Future
  without it =? await self.tds[].ds.query(query), err:
    ret.failure(err)

  var iter = newSharedPtr(QueryIterStore)
  ## does this bypasses SharedPtr isolation? - may need `protect` here?
  iter[].it = it

  var iterWrapper = QueryIter.new()
  iterWrapper.readyForNext = true

  proc next(): Future[?!QueryResponse] {.async.} =
    # print "query:next:start: "
    iterWrapper.finished = iter[].it.finished
    if not iter[].it.finished:
      iterWrapper.readyForNext = false
      query(ret, self.tds, iter)
      await wait(ret)
      iterWrapper.readyForNext = true
      # echo ""
      # print "query:post: ", ret[].results
      # print "query:post:finished: ", iter[].it.finished
      # print "query:post: ", " qrb:key: ", ret[].results.get().key.toString()
      # print "query:post: ", " qrb:data: ", ret[].results.get().data.toString()
      result = ret.convert(QueryResponse)
    else:
      result = success (Key.none, EmptyBytes)

  proc dispose(): Future[?!void] {.async.} =
    iter[].it = nil # ensure our sharedptr doesn't try and dealloc
    ret.release()
    return success()

  iterWrapper.next = next
  iterWrapper.dispose = dispose
  return success iterWrapper

method close*(
  self: ThreadProxyDatastore
): Future[?!void] {.async.} =
  # TODO: how to handle failed close?
  result = success()

  without res =? self.tds[].ds.close(), err:
    result = failure(err)
  # GC_unref(self.tds[].ds) ## TODO: is this needed?

  if self.tds[].tp != nil:
    ## this can block... how to handle? maybe just leak?
    self.tds[].tp.shutdown()

  self[].tds[].ds = nil # ensure our sharedptr doesn't try and dealloc

proc newThreadProxyDatastore*(
  ds: Datastore,
): ?!ThreadProxyDatastore =
  ## create a new 

  var self = ThreadProxyDatastore()
  var value = newSharedPtr(ThreadDatastore)
  # GC_ref(ds) ## TODO: is this needed?

  try:
    value[].ds = ds
    value[].tp = Taskpool.new(num_threads = 2)
  except Exception as exc:
    return err((ref DatastoreError)(msg: exc.msg))

  self.tds = value

  success self
