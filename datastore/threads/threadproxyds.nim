
when not compileOption("threads"):
  {.error: "This module requires --threads:on compilation flag".}

import pkg/upraises

push: {.upraises: [].}

import std/atomics
import std/strutils
import std/tables
import std/sequtils

import pkg/chronos
import pkg/chronos/threadsync
import pkg/questionable
import pkg/questionable/results
import pkg/stew/ptrops
import pkg/taskpools
import pkg/stew/byteutils
import pkg/chronicles

import ../key
import ../query
import ../datastore

import ./asyncsemaphore
import ./databuffer
import ./threadresult

export threadresult

logScope:
  topics = "datastore threadproxyds"

type
  TaskCtx[T: ThreadTypes] = object
    ds: Datastore
    res: ptr ThreadResult[T]
    cancelled: bool
    semaphore: AsyncSemaphore
    signal: ThreadSignalPtr

  ThreadDatastore* = ref object of Datastore
    tp: Taskpool
    ds: Datastore
    semaphore: AsyncSemaphore # semaphore is used for backpressure \
                              # to avoid exhausting file descriptors
    withLocks: bool
    tasks: Table[Key, Future[void]]
    queryLock: AsyncLock      # global query lock, this is only really \
                              # needed for the fsds, but it is expensive!

template withLocks(
  self: ThreadDatastore,
  ctx: TaskCtx,
  key: ?Key = Key.none,
  fut: Future[void],
  body: untyped): untyped =
  try:
    if key.isSome and key.get in self.tasks:
      if self.withLocks:
        await self.tasks[key.get]
      self.tasks[key.get] = fut # we alway want to store the future, but only await if we're using locks

    if self.withLocks:
      await self.queryLock.acquire()  # only lock if it's required (fsds)

    block:
      body
  finally:
    if self.withLocks:
      if key.isSome and key.get in self.tasks:
        self.tasks.del(key.get)
      if self.queryLock.locked:
        self.queryLock.release()

# TODO: needs rework, we can't use `result` with async
template dispatchTask[T](
  self: ThreadDatastore,
  ctx: TaskCtx[T],
  key: ?Key = Key.none,
  runTask: proc): auto =
  try:
    await self.semaphore.acquire()
    let signal = ThreadSignalPtr.new()
    if signal.isErr:
      failure(signal.error)
    else:
      ctx.signal = signal.get()
      let
        fut = wait(ctx.signal)

      withLocks(self, ctx, key, fut):
        runTask()
        await fut
        if ctx.res[].isErr:
          failure ctx.res[].error
        else:
          when result.T isnot void:
            success result.T(ctx.res[].get)
          else:
            success()
  except CancelledError as exc:
    trace "Cancelling thread future!", exc = exc.msg
    ctx.cancelled = true
    await ctx.signal.fire()
    raise exc
  finally:
    discard ctx.signal.close()
    self.semaphore.release()

proc signalMonitor[T](ctx: ptr TaskCtx, fut: Future[T]) {.async.} =
  ## Monitor the signal and cancel the future if
  ## the cancellation flag is set
  ##

  if ctx.isNil:
    trace "ctx is nil"
    return

  try:
    await ctx[].signal.wait()
    trace "Received signal"

    if ctx[].cancelled: # there could eventually be other flags
      trace "Cancelling future"
      if not fut.finished:
        await fut.cancelAndWait() # cancel the `has` future

      discard ctx[].signal.fireSync()
  except CatchableError as exc:
    trace "Exception in thread signal monitor", exc = exc.msg
    ctx[].res[].err(exc)
    discard ctx[].signal.fireSync()

proc asyncHasTask(
  ctx: ptr TaskCtx[bool],
  key: ptr Key) {.async.} =
  defer:
    if not ctx.isNil:
      discard ctx[].signal.fireSync()

  if ctx.isNil:
    trace "ctx is nil"
    return

  let
    fut = ctx[].ds.has(key[])

  asyncSpawn signalMonitor(ctx, fut)
  without ret =? (await fut).catch and res =? ret, error:
    ctx[].res[].err(error)
    return

  ctx[].res[].ok(res)

proc hasTask(ctx: ptr TaskCtx, key: ptr Key) =
  try:
    waitFor asyncHasTask(ctx, key)
  except CatchableError as exc:
    trace "Unexpected exception thrown in asyncHasTask", exc = exc.msg
    raiseAssert exc.msg

method has*(self: ThreadDatastore, key: Key): Future[?!bool] {.async.} =
  var
    res = ThreadResult[bool]()
    ctx = TaskCtx[bool](
      ds: self.ds,
      res: addr res)

  proc runTask() =
    self.tp.spawn hasTask(addr ctx, unsafeAddr key)

  return self.dispatchTask(ctx, key.some, runTask)

proc asyncDelTask(ctx: ptr TaskCtx[void], key: ptr Key) {.async.} =
  defer:
    if not ctx.isNil:
      discard ctx[].signal.fireSync()

  if ctx.isNil:
    trace "ctx is nil"
    return

  let
    fut = ctx[].ds.delete(key[])

  asyncSpawn signalMonitor(ctx, fut)
  without res =? (await fut).catch, error:
    trace "Error in asyncDelTask", error = error.msg
    ctx[].res[].err(error)
    return

  ctx[].res[].ok()
  return

proc delTask(ctx: ptr TaskCtx, key: ptr Key) =
  try:
    waitFor asyncDelTask(ctx, key)
  except CatchableError as exc:
    trace "Unexpected exception thrown in asyncDelTask", exc = exc.msg
    raiseAssert exc.msg

method delete*(
  self: ThreadDatastore,
  key: Key): Future[?!void] {.async.} =
  var
    res = ThreadResult[void]()
    ctx = TaskCtx[void](
      ds: self.ds,
      res: addr res)

  proc runTask() =
    self.tp.spawn delTask(addr ctx, unsafeAddr key)

  return self.dispatchTask(ctx, key.some, runTask)

method delete*(
  self: ThreadDatastore,
  keys: seq[Key]): Future[?!void] {.async.} =

  for key in keys:
    if err =? (await self.delete(key)).errorOption:
      return failure err

  return success()

proc asyncPutTask(
  ctx: ptr TaskCtx[void],
  key: ptr Key,
  data: ptr UncheckedArray[byte],
  len: int) {.async.} =
  defer:
    if not ctx.isNil:
      discard ctx[].signal.fireSync()

  if ctx.isNil:
    trace "ctx is nil"
    return

  let
    fut = ctx[].ds.put(key[], @(data.toOpenArray(0, len - 1)))

  asyncSpawn signalMonitor(ctx, fut)
  without res =? (await fut).catch, error:
    trace "Error in asyncPutTask", error = error.msg
    ctx[].res[].err(error)
    return

  ctx[].res[].ok()

proc putTask(
  ctx: ptr TaskCtx,
  key: ptr Key,
  data: ptr UncheckedArray[byte],
  len: int) =
  ## run put in a thread task
  ##

  try:
    waitFor asyncPutTask(ctx, key, data, len)
  except CatchableError as exc:
    trace "Unexpected exception thrown in asyncPutTask", exc = exc.msg
    raiseAssert exc.msg

method put*(
  self: ThreadDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async.} =
  var
    res = ThreadResult[void]()
    ctx = TaskCtx[void](
      ds: self.ds,
      res: addr res)

  proc runTask() =
    self.tp.spawn putTask(
      addr ctx,
      unsafeAddr key,
      makeUncheckedArray(baseAddr data),
      data.len)

  return self.dispatchTask(ctx, key.some, runTask)

method put*(
  self: ThreadDatastore,
  batch: seq[BatchEntry]): Future[?!void] {.async.} =

  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err

  return success()

proc asyncGetTask(
  ctx: ptr TaskCtx[DataBuffer],
  key: ptr Key) {.async.} =
  defer:
    if not ctx.isNil:
      discard ctx[].signal.fireSync()

  if ctx.isNil:
    trace "ctx is nil"
    return

  let
    fut = ctx[].ds.get(key[])

  asyncSpawn signalMonitor(ctx, fut)
  without res =? (await fut).catch and data =? res, error:
    trace "Error in asyncGetTask", error = error.msg
    ctx[].res[].err(error)
    return

  trace "Got data in get", data
  ctx[].res[].ok(DataBuffer.new(data))

proc getTask(
  ctx: ptr TaskCtx,
  key: ptr Key) =
  ## Run get in a thread task
  ##

  try:
    waitFor asyncGetTask(ctx, key)
  except CatchableError as exc:
    trace "Unexpected exception thrown in asyncGetTask", exc = exc.msg
    raiseAssert exc.msg

method get*(
  self: ThreadDatastore,
  key: Key): Future[?!seq[byte]] {.async.} =
  var
    res = ThreadResult[DataBuffer]()
    ctx = TaskCtx[DataBuffer](
      ds: self.ds,
      res: addr res)

  proc runTask() =
    self.tp.spawn getTask(addr ctx, unsafeAddr key)

  return self.dispatchTask(ctx, key.some, runTask)

method close*(self: ThreadDatastore): Future[?!void] {.async.} =
  for fut in self.tasks.values.toSeq:
    await fut.cancelAndWait() # probably want to store the signal, instead of the future (or both?)

  await self.ds.close()

proc asyncQueryTask(
  ctx: ptr TaskCtx,
  iter: ptr QueryIter) {.async.} =
  defer:
    if not ctx.isNil:
      discard ctx[].signal.fireSync()

  if ctx.isNil or iter.isNil:
    trace "ctx is nil"
    return

  let
    fut = iter[].next()

  asyncSpawn signalMonitor(ctx, fut)
  without ret =? (await fut).catch and res =? ret, error:
    trace "Error in asyncQueryTask", error = error.msg
    ctx[].res[].err(error)
    return

  if res.key.isNone:
    ctx[].res[].ok((default(DataBuffer), default(DataBuffer)))
    return

  var
    keyBuf = DataBuffer.new($(res.key.get()))
    dataBuf = DataBuffer.new(res.data)

  trace "Got query result", key = $res.key.get(), data = res.data
  ctx[].res[].ok((keyBuf, dataBuf))

proc queryTask(
  ctx: ptr TaskCtx,
  iter: ptr QueryIter) =

  try:
    waitFor asyncQueryTask(ctx, iter)
  except CatchableError as exc:
    trace "Unexpected exception thrown in asyncQueryTask", exc = exc.msg
    raiseAssert exc.msg

method query*(
  self: ThreadDatastore,
  query: Query): Future[?!QueryIter] {.async.} =
  without var childIter =? await self.ds.query(query), error:
    return failure error

  var
    iter = QueryIter.new()
    lock = newAsyncLock() # serialize querying under threads

  proc next(): Future[?!QueryResponse] {.async.} =
    defer:
      if lock.locked:
        lock.release()

    trace "About to query"
    if lock.locked:
      return failure (ref DatastoreError)(msg: "Should always await query features")

    await lock.acquire()

    if iter.finished == true:
      return failure (ref QueryEndedError)(msg: "Calling next on a finished query!")

    iter.finished = childIter.finished
    var
      res = ThreadResult[ThreadQueryRes]()
      ctx = TaskCtx[ThreadQueryRes](
        ds: self.ds,
        res: addr res)

    proc runTask() =
      self.tp.spawn queryTask(addr ctx, addr childIter)

    return self.dispatchTask(ctx, Key.none, runTask)

  iter.next = next
  return success iter

proc new*(
  self: type ThreadDatastore,
  ds: Datastore,
  withLocks = static false,
  tp: Taskpool): ?!ThreadDatastore =
  doAssert tp.numThreads > 1, "ThreadDatastore requires at least 2 threads"

  success ThreadDatastore(
    tp: tp,
    ds: ds,
    withLocks: withLocks,
    queryLock: newAsyncLock(),
    semaphore: AsyncSemaphore.new(tp.numThreads - 1))
