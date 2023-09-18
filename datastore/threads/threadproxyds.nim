
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

  ThreadTypes* = void | bool | SomeInteger | DataBuffer | tuple | Atomic
  ThreadResult*[T: ThreadTypes] = Result[T, DataBuffer]

  TaskCtx*[T: ThreadTypes] = object
    head: int
    ds*: ptr Datastore
    res*: ThreadResult[T]
    cancelled: Atomic[bool]
    isActive: Atomic[bool]
    semaphore: AsyncSemaphore
    signal*: ThreadSignalPtr

  ThreadDatastore* = ref object of Datastore
    tp: Taskpool
    ds: Datastore
    semaphore: AsyncSemaphore # semaphore is used for backpressure \
                              # to avoid exhausting file descriptors
    withLocks: bool
    tasks: Table[Key, Future[void]]
    queryLock: AsyncLock      # global query lock, this is only really \
                              # needed for the fsds, but it is expensive!

proc addrOf*[T](ctx: ref TaskCtx[T]): ptr TaskCtx[T] =
  result = cast[ptr TaskCtx[T]](ctx)
  echo "ADDR_OF: ", result.pointer.repr

proc new*[T](
    ctx: typedesc[TaskCtx[T]],
    ds: Datastore,
): ref TaskCtx[T] =
  result = (ref TaskCtx[T])()
  result.ds = unsafeAddr(ds) ##\
    ## doing this appears to break things. previously it was using `addr(ds)`
    ## and reverting to those lets the tests get further. 
    ## 
    ## however, that seems to mean that `addr(ds)` we're taking the 
    ## address of the `var ds: Datastore` location, and not the actual
    ## Datastore:ObjectType. As in `ds: ptr(ref Datastore:ObjectType)`. 
    ## 
    ## so doing the `unsafeAddr` would mean we're taking the ptr location
    ## of the `ds` argument, which is a temporary stack location.
    ## 
    ## not sure how to fix this while using GC types
    ## just taking the address of the var location sorta works, but
    ## crashes now as well, but later. likely due to datastore now being
    ## on two GC heaps?

  echo ""
  echo "TaskCtx:new: ", "addrOf: ", addrOf(result).pointer.repr
  echo "TaskCtx:new: ", "head:ptr: ", unsafeAddr(result.head).pointer.repr
  echo "TaskCtx:new: ", " result:ds:ptr: ", result.ds.pointer.repr

  echo "TaskCtx:new: ds orig:\n\t", ds.repr
  echo "TaskCtx:new:\n\t", " result:repr:\n", result.repr
  echo ""

template withLocks(
  self: ThreadDatastore,
  ctx: ref TaskCtx,
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

# TODO: needs rework, we can't use `result` with async
template dispatchTask(
  self: ThreadDatastore,
  ctx: ref TaskCtx,
  key: ?Key = Key.none,
  runTask: proc): untyped =
  try:
    GC_ref(ctx)
    await self.semaphore.acquire()
    ctx[].signal = ThreadSignalPtr.new().valueOr:
      result = failure(error())
      return

    let
      fut = wait(ctx.signal)

    withLocks(self, ctx, key, fut):
      runTask()
      await fut

      if ctx.res.isErr:
        result = failure(ctx.res.error()) # TODO: fix this, result shouldn't be accessed
  except CancelledError as exc:
    trace "Cancelling thread future!", exc = exc.msg
    if ctx.isActive.load(moAcquireRelease):
      # could do a spinlock here until the other side cancels,
      # but for now it'd at least be better to leak than possibly
      # corrupt memory since it's easier to detect and fix leaks
      # and they won't corrupt random bits of memory
      warn "request was cancelled while thread task is running", exc = exc.msg
      GC_ref(ctx)
    ctx.cancelled.store(true, moAcquireRelease)
    await ctx.signal.fire()
    raise exc
  finally:
    GC_unref(ctx)
    discard ctx.signal.close()
    self.semaphore.release()

proc signalMonitor[T](ctx: ptr TaskCtx, fut: Future[T]) {.async.} =
  ## Monitor the signal and cancel the future if
  ## the cancellation flag is set
  ##

  try:
    ctx[].isActive.store(true, moAcquireRelease)
    await ctx[].signal.wait()
    trace "Received signal"

    if ctx[].cancelled.load(moAcquireRelease): # there could eventually be other flags
      trace "Cancelling future"
      if not fut.finished:
        await fut.cancelAndWait() # cancel the `has` future

      discard ctx[].signal.fireSync()
  except CatchableError as exc:
    trace "Exception in thread signal monitor", exc = exc.msg
    ctx.res.err(exc)
    discard ctx[].signal.fireSync()
  finally:
    ctx[].isActive.store(false, moAcquireRelease)

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
  var

    ctx = TaskCtx[bool].new( ds = self.ds)

  proc runTask() =
    self.tp.spawn hasTask(addrOf(ctx), unsafeAddr key)

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
  var

    ctx = TaskCtx[void].new( ds= self.ds)

  proc runTask() =
    self.tp.spawn delTask(addrOf(ctx), unsafeAddr key)

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

  echo ""
  echo "ASYNC PUT TASK:ptr: ", ctx.pointer.repr
  # echo "PUT TASK:\n", ctx[].repr
  echo "ASYNC PUT TASK:ds: ", ctx[].ds.pointer.repr
  echo "ASYNC PUT TASK:res: ", ctx[].res.repr
  echo "ASYNC PUT TASK:signal: ", ctx[].signal.pointer.repr
  echo "ASYNC PUT TASK:cancelled: ", ctx[].cancelled.repr
  echo "ASYNC PUT TASK:semaphore: ", ctx[].semaphore.repr
  echo "ASYNC PUT TASK:ds: ", ctx[].ds[].repr

  defer:
    discard ctx.signal.fireSync()

  let
    fut = ctx.ds[].put(key[], @(data.toOpenArray(0, len - 1)))

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

  echo ""
  echo "PUT TASK:ptr: ", ctx.pointer.repr
  # echo "PUT TASK:\n", ctx[].repr
  echo "PUT TASK:ds: ", ctx[].ds.pointer.repr
  echo "PUT TASK:res: ", ctx[].res.repr
  echo "PUT TASK:signal: ", ctx[].signal.pointer.repr
  echo "PUT TASK:cancelled: ", ctx[].cancelled.repr
  echo "PUT TASK:semaphore: ", ctx[].semaphore.repr

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
    ctx = TaskCtx[void].new( ds= self.ds)

  proc runTask() =
    self.tp.spawn putTask(
      addrOf(ctx),
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
  var
    ctx = TaskCtx[DataBuffer].new(ds= self.ds)

  proc runTask() =
    self.tp.spawn getTask(addrOf(ctx), unsafeAddr key)

  self.dispatchTask(ctx, key.some, runTask)
  if err =? ctx.res.errorOption:
    return failure err

  return success(@(ctx.res.get()))

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

    trace "About to query"
    if locked:
      return failure (ref DatastoreError)(msg: "Should always await query features")

    locked = true

    if iter.finished == true:
      return failure (ref QueryEndedError)(msg: "Calling next on a finished query!")

    if iter.finished == true:
      return success (Key.none, EmptyBytes)

    var

      ctx = TaskCtx[(bool, DataBuffer, DataBuffer)].new( ds= self.ds)

    proc runTask() =
      self.tp.spawn queryTask(addrOf(ctx), addr childIter)

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
