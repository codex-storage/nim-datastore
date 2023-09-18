
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

type
  ErrorEnum {.pure.} = enum
    DatastoreErr, DatastoreKeyNotFoundErr, CatchableErr

  ThreadTypes = void | bool | SomeInteger | DataBuffer | tuple | Atomic
  ThreadResult[T: ThreadTypes] = Result[T, DataBuffer]

  TaskCtx[T: ThreadTypes] = ref object
    ds: ptr Datastore
    res: ThreadResult[T]
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
  body: untyped) =
  try:
    if key.isSome and key.get in self.tasks:
      if self.withLocks:
        await self.tasks[key.get]
      self.tasks[key.get] = fut # we alway want to store the future, but only await if we're using locks

    if self.withLocks:
      await self.queryLock.acquire()  # only lock if it's required (fsds)

    body
  finally:
    if self.withLocks:
      if key.isSome and key.get in self.tasks:
        self.tasks.del(key.get)
      if self.queryLock.locked:
        self.queryLock.release()

template dispatchTask(
  self: ThreadDatastore,
  ctx: TaskCtx,
  key: ?Key = Key.none,
  runTask: proc): untyped =

  let
    fut = wait(ctx.signal)

  withLocks(self, ctx, key, fut):
    try:
      GC_ref(ctx)
      runTask()
      await fut

      if ctx.res.isErr:
        result = failure(ctx.res.error()) # TODO: fix this, result shouldn't be accessed
    except CancelledError as exc:
      trace "Cancelling thread future!", exc = exc.msg
      ctx.cancelled = true
      await ctx.signal.fire()
      raise exc
    finally:
      discard ctx.signal.close()

proc signalMonitor[T](ctx: ptr TaskCtx, fut: Future[T]) {.async.} =
  ## Monitor the signal and cancel the future if
  ## the cancellation flag is set
  ##

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
    ctx.res.err(exc)
    discard ctx[].signal.fireSync()

proc asyncHasTask(
  ctx: ptr TaskCtx[bool],
  key: ptr Key) {.async.} =
  defer:
    discard ctx[].signal.fireSync()

  let
    fut = ctx[].ds[].has(key[])

  asyncSpawn signalMonitor(ctx, fut)
  without ret =? (await fut).catch and res =? ret, error:
    ctx.res.err(error)
    return

  ctx.res.ok(res)

proc hasTask(ctx: ptr TaskCtx, key: ptr Key) =
  try:
    waitFor asyncHasTask(ctx, key)
  except CatchableError as exc:
    trace "Unexpected exception thrown in asyncHasTask", exc = exc.msg
    raiseAssert exc.msg

method has*(self: ThreadDatastore, key: Key): Future[?!bool] {.async.} =
  defer:
    self.semaphore.release()

  await self.semaphore.acquire()

  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure(error())

    ctx = TaskCtx[bool](
      ds: addr self.ds,
      signal: signal)

  proc runTask() =
    self.tp.spawn hasTask(addr ctx, unsafeAddr key)

  self.dispatchTask(ctx, key.some, runTask)
  return success(ctx.res.get())

proc asyncDelTask(ctx: ptr TaskCtx[void], key: ptr Key) {.async.} =
  defer:
    discard ctx[].signal.fireSync()

  let
    fut = ctx[].ds[].delete(key[])

  asyncSpawn signalMonitor(ctx, fut)
  without res =? (await fut).catch, error:
    trace "Error in asyncDelTask", error = error.msg
    ctx.res.err(error)
    return

  ctx.res.ok()
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
  defer:
    self.semaphore.release()

  await self.semaphore.acquire()

  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure(error())

    ctx = TaskCtx[void](
      ds: addr self.ds,
      signal: signal)

  proc runTask() =
    self.tp.spawn delTask(addr ctx, unsafeAddr key)

  self.dispatchTask(ctx, key.some, runTask)
  return success()

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
    discard ctx[].signal.fireSync()

  let
    fut = ctx[].ds[].put(key[], @(data.toOpenArray(0, len - 1)))

  asyncSpawn signalMonitor(ctx, fut)
  without res =? (await fut).catch, error:
    trace "Error in asyncPutTask", error = error.msg
    ctx.res.err(error)
    return

  ctx.res.ok()

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
  defer:
    self.semaphore.release()

  await self.semaphore.acquire()

  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure(error())

    ctx = TaskCtx[void](
      ds: addr self.ds,
      signal: signal)

  proc runTask() =
    self.tp.spawn putTask(
      addr ctx,
      unsafeAddr key,
      makeUncheckedArray(baseAddr data),
      data.len)

  self.dispatchTask(ctx, key.some, runTask)
  return success()

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
    discard ctx[].signal.fireSync()

  let
    fut = ctx[].ds[].get(key[])

  asyncSpawn signalMonitor(ctx, fut)
  without res =? (await fut).catch and data =? res, error:
    trace "Error in asyncGetTask", error = error.msg
    ctx.res.err(error)
    return

  ctx.res.ok(DataBuffer.new(data))

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
  defer:
    self.semaphore.release()

  await self.semaphore.acquire()

  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure(error())

  var
    ctx = TaskCtx[DataBuffer](
      ds: addr self.ds,
      signal: signal)

  proc runTask() =
    self.tp.spawn getTask(addr ctx, unsafeAddr key)

  self.dispatchTask(ctx, key.some, runTask)
  if err =? res.errorOption:
    return failure err

  return success(@(res.get()))

method close*(self: ThreadDatastore): Future[?!void] {.async.} =
  for fut in self.tasks.values.toSeq:
    await fut.cancelAndWait() # probably want to store the signal, instead of the future (or both?)

  await self.ds.close()

proc asyncQueryTask(
  ctx: ptr TaskCtx,
  iter: ptr QueryIter) {.async.} =
  defer:
    discard ctx[].signal.fireSync()

  let
    fut = iter[].next()

  asyncSpawn signalMonitor(ctx, fut)
  without ret =? (await fut).catch and res =? ret, error:
    trace "Error in asyncQueryTask", error = error.msg
    ctx.res.err(error)
    return

  if res.key.isNone:
    ctx.res.ok((false, default(DataBuffer), default(DataBuffer)))
    return

  var
    keyBuf = DataBuffer.new($(res.key.get()))
    dataBuf = DataBuffer.new(res.data)

  ctx.res.ok((true, keyBuf, dataBuf))

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
    locked = false

  proc next(): Future[?!QueryResponse] {.async.} =
    defer:
      locked = false
      self.semaphore.release()

    trace "About to query"
    await self.semaphore.acquire()
    if locked:
      return failure (ref DatastoreError)(msg: "Should always await query features")

    locked = true

    if iter.finished == true:
      return failure (ref QueryEndedError)(msg: "Calling next on a finished query!")

    if iter.finished == true:
      return success (Key.none, EmptyBytes)

    var
      signal = ThreadSignalPtr.new().valueOr:
        return failure("Failed to create signal")

      ctx = TaskCtx[(bool, DataBuffer, DataBuffer)](
        ds: addr self.ds,
        signal: signal)

    proc runTask() =
      self.tp.spawn queryTask(addr ctx, addr childIter)

    self.dispatchTask(ctx, Key.none, runTask)
    if err =? ctx.res.errorOption:
      trace "Query failed", err = err
      return failure err

    let (ok, key, data) = ctx.res.get()
    if not ok:
      iter.finished = true
      return success (Key.none, EmptyBytes)

    return success (Key.init($key).expect("should not fail").some, @(data))

  iter.next = next
  return success iter

proc new*(
  self: type ThreadDatastore,
  ds: Datastore,
  withLocks = static false,
  tp: Taskpool): ?!ThreadDatastore =
  doAssert tp.numThreads > 1, "ThreadDatastore requires at least 2 threads"

  case withLocks:
  of true:
    success ThreadDatastore(
      tp: tp,
      ds: ds,
      withLocks: true,
      queryLock: newAsyncLock(),
      semaphore: AsyncSemaphore.new(tp.numThreads - 1))
  else:
    success ThreadDatastore(
      tp: tp,
      ds: ds,
      withLocks: false,
      semaphore: AsyncSemaphore.new(tp.numThreads - 1))
