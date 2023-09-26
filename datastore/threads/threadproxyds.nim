
when not compileOption("threads"):
  {.error: "This module requires --threads:on compilation flag".}

import pkg/upraises

push: {.upraises: [].}

import std/tables
import std/locks

import pkg/chronos
import pkg/chronos/threadsync
import pkg/questionable
import pkg/questionable/results
import pkg/taskpools
import pkg/chronicles

import ../key
import ../query
import ../datastore
import ../backend
import ../sql/sqliteds

import ./asyncsemaphore
import ./databuffer
import ./threadresult

export threadresult

logScope:
  topics = "datastore threadproxyds"

type
  SqliteDB = SQLiteBackend[KeyId,DataBuffer]

  ThreadBackendKinds* = enum
    Sqlite
    # Filesystem

  ThreadBackend* = object
    case kind*: ThreadBackendKinds
    of Sqlite:
      sql*: SQLiteBackend[KeyId,DataBuffer]

  TaskCtx[D; T: ThreadTypes] = object
    res: ThreadResult[T]
    signal: ThreadSignalPtr
    running: bool
    cancelled: bool

  ThreadDatastore* = ref object of Datastore
    tp: Taskpool
    backend: ThreadBackend
    semaphore: AsyncSemaphore # semaphore is used for backpressure \
                              # to avoid exhausting file descriptors

var ctxLock: Lock
ctxLock.initLock()

proc setCancelled(ctx: var TaskCtx): bool =
  withLock(ctxLock):
    if ctx.running:
      return false
    else:
      ctx.cancelled = true
      return true

proc acquireSignal(): ?!ThreadSignalPtr =
  let signal = ThreadSignalPtr.new()
  if signal.isErr():
    failure (ref CatchableError)(msg: "failed to aquire ThreadSignalPtr: " & signal.error())
  else:
    success signal.get()

template dispatchTask[T](self: ThreadDatastore,
                          signal: ThreadSignalPtr,
                          blk: untyped
                        ): auto =
  var
    ctx {.inject.} = TaskCtx[SqliteDB, T](signal: signal)
  try:
    case self.backend.kind:
    of Sqlite:
      var ds {.inject.} = self.backend.sql
      proc runTask() =
        `blk`
      runTask()

      await wait(ctx.signal)
  except CancelledError as exc:
    trace "Cancelling thread future!", exc = exc.msg
    while not ctx.setCancelled():
      warn "waiting to cancel thread future!", fn = astToStr(fn)
      await sleepAsync(10.milliseconds)
    raise exc
  finally:
    discard ctx.signal.close()
    self.semaphore.release()

template executeTask(ctx: ptr TaskCtx, blk: untyped) =
  try:
    withLock(ctxLock):
      if ctx.cancelled:
        return
      ctx.running = true
    
    ## run backend command
    let res = `blk`

    withLock(ctxLock):
      ctx.running = false
      ctx.res = res.mapErr() do(e: ref CatchableError) -> ThreadResErr:
        e.toThreadErr()
  except CatchableError as exc:
    trace "Unexpected exception thrown in asyncHasTask", exc = exc.msg
    raiseAssert exc.msg
  finally:
    discard ctx[].signal.fireSync()

proc hasTask[DB](ctx: ptr TaskCtx, ds: DB, key: KeyId) {.gcsafe.} =
  ## run backend command
  executeTask(ctx):
    has(ds, key)

method has*(self: ThreadDatastore,
            key: Key): Future[?!bool] {.async.} =
  await self.semaphore.acquire()
  without signal =? acquireSignal(), err:
    return failure err

  let key = KeyId.new key.id()
  dispatchTask[bool](self, signal):
    self.tp.spawn hasTask(addr ctx, ds, key)

proc deleteTask[DB](ctx: ptr TaskCtx, ds: DB;
                    key: KeyId) {.gcsafe.} =
  ## run backend command
  executeTask(ctx):
    delete(ds, key)

method delete*(self: ThreadDatastore,
               key: Key): Future[?!void] {.async.} =
  await self.semaphore.acquire()
  without signal =? acquireSignal(), err:
    return failure err

  let key = KeyId.new key.id()
  dispatchTask[void](self, signal):
    self.tp.spawn deleteTask(addr ctx, ds, key)

method delete*(self: ThreadDatastore,
               keys: seq[Key]): Future[?!void] {.async.} =

  for key in keys:
    if err =? (await self.delete(key)).errorOption:
      return failure err

  return success()

# proc asyncPutTask(
#   ctx: ptr TaskCtx[void],
#   key: ptr Key,
#   data: ptr UncheckedArray[byte],
#   len: int) {.async.} =

#   if ctx.isNil:
#     trace "ctx is nil"
#     return

#   let
#     key = key[]
#     data = @(data.toOpenArray(0, len - 1))
#     fut = ctx[].ds.put(key, data)

#   asyncSpawn signalMonitor(ctx, fut)
#   without res =? (await fut).catch, error:
#     trace "Error in asyncPutTask", error = error.msg
#     ctx[].res[].err(error)
#     return

#   ctx[].res[].ok()

# proc putTask(
#   ctx: ptr TaskCtx,
#   key: ptr Key,
#   data: ptr UncheckedArray[byte],
#   len: int) =
#   ## run put in a thread task
#   ##

#   defer:
#     if not ctx.isNil:
#       discard ctx[].signal.fireSync()

#   try:
#     waitFor asyncPutTask(ctx, key, data, len)
#   except CatchableError as exc:
#     trace "Unexpected exception thrown in asyncPutTask", exc = exc.msg
#     raiseAssert exc.msg

# method put*(
#   self: ThreadDatastore,
#   key: Key,
#   data: seq[byte]): Future[?!void] {.async.} =
#   var
#     key = key
#     data = data
#     res = ThreadResult[void]()
#     ctx = TaskCtx[void](
#       ds: self.ds,
#       res: addr res)

#   proc runTask() =
#     self.tp.spawn putTask(
#       addr ctx,
#       addr key,
#       makeUncheckedArray(addr data[0]),
#       data.len)

#   return self.dispatchTask(ctx, key.some, runTask)

# method put*(
#   self: ThreadDatastore,
#   batch: seq[BatchEntry]): Future[?!void] {.async.} =

#   for entry in batch:
#     if err =? (await self.put(entry.key, entry.data)).errorOption:
#       return failure err

#   return success()

# proc asyncGetTask(
#   ctx: ptr TaskCtx[DataBuffer],
#   key: ptr Key) {.async.} =
#   if ctx.isNil:
#     trace "ctx is nil"
#     return

#   let
#     key = key[]
#     fut = ctx[].ds.get(key)

#   asyncSpawn signalMonitor(ctx, fut)
#   without res =? (await fut).catch and data =? res, error:
#     trace "Error in asyncGetTask", error = error.msg
#     ctx[].res[].err(error)
#     return

#   trace "Got data in get"
#   ctx[].res[].ok(DataBuffer.new(data))

# proc getTask(
#   ctx: ptr TaskCtx,
#   key: ptr Key) =
#   ## Run get in a thread task
#   ##

#   defer:
#     if not ctx.isNil:
#       discard ctx[].signal.fireSync()

#   try:
#     waitFor asyncGetTask(ctx, key)
#   except CatchableError as exc:
#     trace "Unexpected exception thrown in asyncGetTask", exc = exc.msg
#     raiseAssert exc.msg

# method get*(
#   self: ThreadDatastore,
#   key: Key): Future[?!seq[byte]] {.async.} =
#   var
#     key = key
#     res = ThreadResult[DataBuffer]()
#     ctx = TaskCtx[DataBuffer](
#       ds: self.ds,
#       res: addr res)

#   proc runTask() =
#     self.tp.spawn getTask(addr ctx, addr key)

#   return self.dispatchTask(ctx, key.some, runTask)

# method close*(self: ThreadDatastore): Future[?!void] {.async.} =
#   for fut in self.tasks.values.toSeq:
#     await fut.cancelAndWait() # probably want to store the signal, instead of the future (or both?)

#   await self.ds.close()

# proc asyncQueryTask(
#   ctx: ptr TaskCtx,
#   iter: ptr QueryIter) {.async.} =

#   if ctx.isNil or iter.isNil:
#     trace "ctx is nil"
#     return

#   let
#     fut = iter[].next()

#   asyncSpawn signalMonitor(ctx, fut)
#   without ret =? (await fut).catch and res =? ret, error:
#     trace "Error in asyncQueryTask", error = error.msg
#     ctx[].res[].err(error)
#     return

#   if res.key.isNone:
#     ctx[].res[].ok((default(DataBuffer), default(DataBuffer)))
#     return

#   var
#     keyBuf = DataBuffer.new($(res.key.get()))
#     dataBuf = DataBuffer.new(res.data)

#   trace "Got query result", key = $res.key.get(), data = res.data
#   ctx[].res[].ok((keyBuf, dataBuf))

# proc queryTask(
#   ctx: ptr TaskCtx,
#   iter: ptr QueryIter) =

#   defer:
#     if not ctx.isNil:
#       discard ctx[].signal.fireSync()

#   try:
#     waitFor asyncQueryTask(ctx, iter)
#   except CatchableError as exc:
#     trace "Unexpected exception thrown in asyncQueryTask", exc = exc.msg
#     raiseAssert exc.msg

# method query*(
#   self: ThreadDatastore,
#   query: Query): Future[?!QueryIter] {.async.} =
#   without var childIter =? await self.ds.query(query), error:
#     return failure error

#   var
#     iter = QueryIter.new()
#     lock = newAsyncLock() # serialize querying under threads

#   proc next(): Future[?!QueryResponse] {.async.} =
#     defer:
#       if lock.locked:
#         lock.release()

#     trace "About to query"
#     if lock.locked:
#       return failure (ref DatastoreError)(msg: "Should always await query features")

#     await lock.acquire()

#     if iter.finished == true:
#       return failure (ref QueryEndedError)(msg: "Calling next on a finished query!")

#     iter.finished = childIter.finished
#     var
#       res = ThreadResult[QueryResponse]()
#       ctx = TaskCtx[QueryResponse](
#         ds: self.ds,
#         res: addr res)

#     proc runTask() =
#       self.tp.spawn queryTask(addr ctx, addr childIter)

#     return self.dispatchTask(ctx, Key.none, runTask)

#   iter.next = next
#   return success iter

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
