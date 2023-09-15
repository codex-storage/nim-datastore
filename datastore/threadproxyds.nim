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

  let sig = SharedSignal.new()
  await sig.acquireSig()

  var ret = newThreadResult(bool)
  proc submitHas() =
    let bkey = KeyBuffer.new(key)
    self.tds[].tp.spawn hasTask(sig, ret, self.tds, bkey)

  submitHas()
  await sig.wait()
  return ret.convert(bool)

method delete*(
  self: ThreadProxyDatastore,
  key: Key
): Future[?!void] {.async.} =

  let sig = SharedSignal.new()
  await sig.acquireSig()

  var ret = newThreadResult(void)
  proc submitDelete() =
    let bkey = KeyBuffer.new(key)
    self.tds[].tp.spawn deleteTask(sig, ret, self.tds, bkey)

  submitDelete()
  await sig.wait()
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

  let sig = SharedSignal.new()
  await sig.acquireSig()

  var ret = newThreadResult(ValueBuffer)
  proc submitGet() =
    let bkey = KeyBuffer.new(key)
    self.tds[].tp.spawn getTask(sig, ret, self.tds, bkey)

  submitGet()
  await sig.wait()
  return ret.convert(seq[byte])


method put*(
  self: ThreadProxyDatastore,
  key: Key,
  data: seq[byte]
): Future[?!void] {.async.} =

  echoed "put request args: ", $getThreadId()
  let sig = SharedSignal.new()
  await sig.acquireSig()

  let ret = newSharedPtr(ThreadResult[void])
  proc submitPut(): ?!void =
    let bkey = KeyBuffer.new(key)
    let bval = DataBuffer.new(data)
    # queue taskpool work
    self.tds[].tp.spawn putTask(sig, ret, self.tds, bkey, bval)
  
  submitPut()
  await sig.wait()
  return ret.convert(void)


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

method query*(
  self: ThreadProxyDatastore,
  query: Query
): Future[?!QueryIter] {.async.} =

  let sig = SharedSignal.new()
  await sig.acquireSig()
  var ret = newThreadResult(QueryResponseBuffer)

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
      sig.query(ret, self.tds, iter)
      await sig.wait()
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
  self[].tds[].tp = nil # ensure our sharedptr doesn't try and dealloc

  self[].tds[].ds = nil # ensure our sharedptr doesn't try and dealloc
  self.tds.release()
  echo "close done"
  # dispose(dsCell)

proc newThreadProxyDatastore*(
  ds: Datastore,
): ?!ThreadProxyDatastore =
  ## create a new 

  var self = ThreadProxyDatastore()
  var value = newSharedPtr(ThreadDatastore)
  # let dsCell = protect(cast[pointer](ds))
  # GC_ref(ds) ## TODO: is this needed?

  try:
    value[].ds = ds
    value[].tp = Taskpool.new(num_threads = 2)
  except Exception as exc:
    return err((ref DatastoreError)(msg: exc.msg))

  self.tds = value

  success self
