
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

  ThreadBackendKinds* = enum
    Sqlite
    # Filesystem

  ThreadBackend* = object
    case kind*: ThreadBackendKinds
    of Sqlite:
      sql*: SQLiteBackend[KeyId,DataBuffer]

  TaskCtx[T: ThreadTypes] = object
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

proc setRunning[T](ctx: ptr TaskCtx[T]): bool =
  withLock(ctxLock):
    if ctx.cancelled:
      return
    ctx.running = true
proc setDone[T](ctx: ptr TaskCtx[T]) =
  withLock(ctxLock):
    ctx.running = false

proc acquireSignal(): ?!ThreadSignalPtr =
  let signal = ThreadSignalPtr.new()
  if signal.isErr():
    failure (ref CatchableError)(msg: "failed to aquire ThreadSignalPtr: " & signal.error())
  else:
    success signal.get()

template executeTask[T](ctx: ptr TaskCtx[T], blk: untyped) =
  try:
    if not ctx.setRunning():
      return
    
    ## run backend command
    let res = `blk`
    if res.isOk():
      when T is void:
        ctx.res.ok()
      else:
        ctx.res.ok(res.get())
    else:
      ctx.res.err res.error().toThreadErr()
  except CatchableError as exc:
    trace "Unexpected exception thrown in async task", exc = exc.msg
    ctx[].res.err exc.toThreadErr()
  finally:
    ctx.setDone()
    discard ctx[].signal.fireSync()

template dispatchTask[T](self: ThreadDatastore,
                          signal: ThreadSignalPtr,
                          blk: untyped
                        ): auto =
  var
    ctx {.inject.} = TaskCtx[T](signal: signal)
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

proc hasTask[T, DB](ctx: ptr TaskCtx[T], ds: DB, key: KeyId) {.gcsafe.} =
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

proc deleteTask[T, DB](ctx: ptr TaskCtx[T], ds: DB;
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

proc putTask[T, DB](ctx: ptr TaskCtx[T], ds: DB;
                  key: KeyId,
                  data: DataBuffer) {.gcsafe, nimcall.} =
  ## run backend command
  executeTask(ctx):
    put(ds, key, data)

method put*(self: ThreadDatastore,
            key: Key,
            data: seq[byte]): Future[?!void] {.async.} =
  await self.semaphore.acquire()
  without signal =? acquireSignal(), err:
    return failure err

  let key = KeyId.new key.id()
  let data = DataBuffer.new data
  dispatchTask[void](self, signal):
    self.tp.spawn putTask(addr ctx, ds, key, data)
  
method put*(
  self: ThreadDatastore,
  batch: seq[BatchEntry]): Future[?!void] {.async.} =

  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err

  return success()

proc getTask[T, DB](ctx: ptr TaskCtx[T], ds: DB;
                 key: KeyId) {.gcsafe, nimcall.} =
  ## run backend command
  executeTask(ctx):
    get(ds, key)

method get*(self: ThreadDatastore,
            key: Key,
            ): Future[?!seq[byte]] {.async.} =
  await self.semaphore.acquire()
  without signal =? acquireSignal(), err:
    return failure err

  let key = KeyId.new key.id()
  dispatchTask[void](self, signal):
    self.tp.spawn getTask(addr ctx, ds, key)

method close*(self: ThreadDatastore): Future[?!void] {.async.} =
  await self.semaphore.closeAll()
  case self.backend.kind:
  of Sqlite:
    self.backend.sql.close()

type
  QResult = DbQueryResponse[KeyId, DataBuffer]

proc queryTask[DB](
    ctx: ptr TaskCtx[QResult],
    ds: DB,
    dq: DbQuery[KeyId]
) {.gcsafe, nimcall.} =
  ## run query command
  if not ctx.setRunning():
    return

  var qh = ds.query(dq)
  if qh .isOk():
    (?!QResult).ok(default(QResult))
  else:
    (?!QResult).err(qh.error())

  var handle = qh.get()

  for item in 
  executeTask(ctx):
    query(ds, key)

method query*(
  self: ThreadDatastore,
  query: Query): Future[?!QueryIter] {.async.} =

  await self.semaphore.acquire()
  without signal =? acquireSignal(), err:
    return failure err

  let dq = dbQuery(
    key= KeyId.new query.key.id(),
    value=query.value,
    limit=query.limit,
    offset=query.offset,
    sort=query.sort,
  )

  dispatchTask[DbQueryResponse[KeyId, DataBuffer]](self, signal):
    self.tp.spawn queryTask(addr ctx, ds, dq)

  var
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
      res = ThreadResult[QueryResponse]()


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
