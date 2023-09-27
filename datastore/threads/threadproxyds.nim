
when not compileOption("threads"):
  {.error: "This module requires --threads:on compilation flag".}

import pkg/upraises

push: {.upraises: [].}

import std/tables
import std/locks
import std/sugar


import pkg/chronos
import pkg/chronos/threadsync
import pkg/questionable
import pkg/questionable/results
import pkg/taskpools
import pkg/chronicles
import pkg/threading/smartptrs

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

  TaskCtxObj*[T: ThreadTypes] = object
    res: ThreadResult[T]
    signal: ThreadSignalPtr
    running: bool
    cancelled: bool

  TaskCtx*[T] = SharedPtr[TaskCtxObj[T]]

  ThreadDatastore* = ref object of Datastore
    tp: Taskpool
    backend: ThreadBackend
    semaphore: AsyncSemaphore # semaphore is used for backpressure \
                              # to avoid exhausting file descriptors

var ctxLock: Lock
ctxLock.initLock()

proc setCancelled[T](ctx: TaskCtx[T]) =
  withLock(ctxLock):
    ctx[].cancelled = true

proc setRunning[T](ctx: TaskCtx[T]): bool =
  withLock(ctxLock):
    if ctx[].cancelled:
      return false
    ctx[].running = true
    return true
proc setDone[T](ctx: TaskCtx[T]) =
  withLock(ctxLock):
    ctx[].running = false

proc acquireSignal(): ?!ThreadSignalPtr =
  let signal = ThreadSignalPtr.new()
  if signal.isErr():
    failure (ref CatchableError)(msg: "failed to aquire ThreadSignalPtr: " & signal.error())
  else:
    success signal.get()

template executeTask[T](ctx: TaskCtx[T], blk: untyped) =
  try:
    if not ctx.setRunning():
      return
    
    ## run backend command
    let res = `blk`
    if res.isOk():
      when T is void:
        ctx[].res.ok()
      else:
        ctx[].res.ok(res.get())
    else:
      ctx[].res.err res.error().toThreadErr()

  except CatchableError as exc:
    trace "Unexpected exception thrown in async task", exc = exc.msg
    ctx[].res.err exc.toThreadErr()
  except Exception as exc:
    trace "Unexpected defect thrown in async task", exc = exc.msg
    ctx[].res.err exc.toThreadErr()
  finally:
    ctx.setDone()
    discard ctx[].signal.fireSync()

template dispatchTaskWrap[T](self: ThreadDatastore,
                          signal: ThreadSignalPtr,
                          blk: untyped
                        ): auto =
  try:
    case self.backend.kind:
    of Sqlite:
      var ds {.used, inject.} = self.backend.sql
      proc runTask() =
        `blk`
      runTask()
      await wait(ctx[].signal)

  except CancelledError as exc:
    trace "Cancelling thread future!", exc = exc.msg
    ctx.setCancelled()
    raise exc
  finally:
    discard ctx[].signal.close()
    self.semaphore.release()

template dispatchTask[T](self: ThreadDatastore,
                          signal: ThreadSignalPtr,
                          blk: untyped
                        ): auto =
  let ctx {.inject.} = newSharedPtr(TaskCtxObj[T](signal: signal))
  dispatchTaskWrap[T](self, signal, blk)


proc hasTask[T, DB](ctx: TaskCtx[T], ds: DB, key: KeyId) {.gcsafe.} =
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
    self.tp.spawn hasTask(ctx, ds, key)
  return ctx[].res.toRes(v => v)


proc deleteTask[T, DB](ctx: TaskCtx[T], ds: DB;
                       key: KeyId) {.gcsafe.} =
  ## run backend command
  executeTask(ctx):
    delete(ds, key)

method delete*(self: ThreadDatastore,
               key: Key): Future[?!void] {.async.} =
  ## delete key
  await self.semaphore.acquire()
  without signal =? acquireSignal(), err:
    return failure err

  let key = KeyId.new key.id()
  dispatchTask[void](self, signal):
    self.tp.spawn deleteTask(ctx, ds, key)

  return ctx[].res.toRes()

method delete*(self: ThreadDatastore,
               keys: seq[Key]): Future[?!void] {.async.} =
  ## delete batch
  for key in keys:
    if err =? (await self.delete(key)).errorOption:
      return failure err

  return success()


proc putTask[T, DB](ctx: TaskCtx[T], ds: DB;
                    key: KeyId,
                    data: DataBuffer) {.gcsafe, nimcall.} =
  executeTask(ctx):
    put(ds, key, data)

method put*(self: ThreadDatastore,
            key: Key,
            data: seq[byte]): Future[?!void] {.async.} =
  ## put key with data
  await self.semaphore.acquire()
  without signal =? acquireSignal(), err:
    return failure err

  dispatchTask[void](self, signal):
    let key = KeyId.new key.id()
    let data = DataBuffer.new data
    self.tp.spawn putTask(ctx, ds, key, data)

  return ctx[].res.toRes()
  
method put*(
  self: ThreadDatastore,
  batch: seq[BatchEntry]): Future[?!void] {.async.} =
  ## put batch data
  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err
  
  return success()


proc getTask[DB](ctx: TaskCtx[DataBuffer], ds: DB;
                 key: KeyId) {.gcsafe, nimcall.} =
  ## run backend command
  executeTask(ctx):
    let res = get(ds, key)
    res

method get*(self: ThreadDatastore,
            key: Key,
            ): Future[?!seq[byte]] {.async.} =
  await self.semaphore.acquire()
  without signal =? acquireSignal(), err:
    return failure err

  let key = KeyId.new key.id()
  dispatchTask[DataBuffer](self, signal):
    self.tp.spawn getTask(ctx, ds, key)

  return ctx[].res.toRes(v => v.toSeq())

method close*(self: ThreadDatastore): Future[?!void] {.async.} =
  await self.semaphore.closeAll()
  case self.backend.kind:
  of Sqlite:
    self.backend.sql.close()

type
  QResult = DbQueryResponse[KeyId, DataBuffer]

proc queryTask[DB](
    ctx: TaskCtx[QResult],
    ds: DB,
    dq: DbQuery[KeyId]
) {.gcsafe, nimcall.} =
  ## run query command
  echo "\n\tqueryTask:init"
  executeTask(ctx):
    echo "\tqueryTask:exec:"
    # we execute this all inside `executeTask`
    # so we need to return a final result
    let qh = ds.query(dq)
    echo "\tqueryTask:query: ", qh
    if qh.isErr():
      # set error and exit executeTask, which will fire final signal
      echo "\tqueryTask:query:err "
      (?!QResult).err(qh.error())
    else:
      echo "\tqueryTask:query:ok "
      # otherwise manually an set empty ok result
      ctx[].res.ok (KeyId.none, DataBuffer(), )
      discard ctx[].signal.fireSync()
      echo "\tqueryTask:query:fireSync "

      var handle = qh.get()
      for item in handle.iter():
        # wait for next request from async thread
        echo "\tqueryTask:query:iter:wait "
        discard ctx[].signal.waitSync().get()

        if ctx[].cancelled:
          # cancel iter, then run next cycle so it'll finish and close
          handle.cancel = true
          continue
        else:
          ctx[].res = item.mapErr() do(exc: ref CatchableError) -> ThreadResErr:
            exc
          echo "\tqueryTask:query:iter:fireSync "
          discard ctx[].signal.fireSync()

      # set final result
      (?!QResult).ok((KeyId.none, DataBuffer()))

method query*(
  self: ThreadDatastore,
  q: Query): Future[?!QueryIter] {.async.} =

  echo "\nquery:"
  await self.semaphore.acquire()
  without signal =? acquireSignal(), err:
    return failure err

  echo "query:dbQuery:"
  let dq = dbQuery(
    key= KeyId.new q.key.id(),
    value=q.value, limit=q.limit, offset=q.offset, sort=q.sort)

  echo "query:init:dispatch:"
  dispatchTask[DbQueryResponse[KeyId, DataBuffer]](self, signal):
    echo "query:init:dispatch:queryTask"
    self.tp.spawn queryTask(ctx, ds, dq)

  echo "query:init:dispatch:queryTask:done"
  var
    lock = newAsyncLock() # serialize querying under threads
    iter = QueryIter.new()

  echo "query:asyncLock:done"
  echo "query:next:ready: "

  proc next(): Future[?!QueryResponse] {.async.} =
    echo "query:next:exec: "
    let ctx = ctx
    defer:
      if lock.locked:
        lock.release()

    trace "About to query"
    if lock.locked:
      return failure (ref DatastoreError)(msg: "Should always await query features")
    if iter.finished == true:
      return failure (ref QueryEndedError)(msg: "Calling next on a finished query!")

    await lock.acquire()

    dispatchTaskWrap[DbQueryResponse[KeyId, DataBuffer]](self, signal):
      # trigger query task to iterate then wait for new result!
      discard ctx[].signal.fireSync()

    if not ctx[].running:
      iter.finished = true

    if ctx[].res.isErr():
      return err(ctx[].res.error())
    else:
      let qres = ctx[].res.get()
      let key = qres.key.map(proc (k: KeyId): Key = k.toKey())
      let data = qres.data.toSeq()
      return (?!QueryResponse).ok((key: key, data: data))

  iter.next = next
  return success iter

proc new*[DB](self: type ThreadDatastore,
          db: DB,
          withLocks = static false,
          tp: Taskpool
          ): ?!ThreadDatastore =
  doAssert tp.numThreads > 1, "ThreadDatastore requires at least 2 threads"

  when DB is SQLiteBackend[KeyId,DataBuffer]:
    let backend = ThreadBackend(kind: Sqlite, sql: db)
  else:
    {.error: "unsupported backend: " & $typeof(db).}

  success ThreadDatastore(
    tp: tp,
    backend: backend,
    # withLocks: withLocks,
    # queryLock: newAsyncLock(),
    semaphore: AsyncSemaphore.new(tp.numThreads - 1)
  )
