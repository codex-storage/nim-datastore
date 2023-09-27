
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
    nextSignal: (Lock, Cond)

  TaskCtx*[T] = SharedPtr[TaskCtxObj[T]]

  ThreadDatastore* = ref object of Datastore
    tp: Taskpool
    backend: ThreadBackend
    semaphore: AsyncSemaphore # semaphore is used for backpressure \
                              # to avoid exhausting file descriptors

var ctxLock: Lock
ctxLock.initLock()

proc setCancelled[T](ctx: TaskCtx[T]) =
  # withLock(ctxLock):
    ctx[].cancelled = true

proc setRunning[T](ctx: TaskCtx[T]): bool =
  # withLock(ctxLock):
    if ctx[].cancelled:
      return false
    ctx[].running = true
    return true
proc setDone[T](ctx: TaskCtx[T]) =
  # withLock(ctxLock):
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
  # except Exception as exc:
  #   trace "Unexpected defect thrown in async task", exc = exc.msg
  #   ctx[].res.err exc.toThreadErr()
  finally:
    ctx.setDone()
    # echo "\t\texecuteTask:fireSync!"
    discard ctx[].signal.fireSync()

template dispatchTaskWrap[T](self: ThreadDatastore,
                          signal: ThreadSignalPtr,
                          blk: untyped
                        ): auto =
  case self.backend.kind:
  of Sqlite:
    var ds {.used, inject.} = self.backend.sql
    proc runTask() =
      `blk`
    runTask()
    # echo "\t\tdispatchTask:wait!"
    await wait(ctx[].signal)

template dispatchTask[T](self: ThreadDatastore,
                          signal: ThreadSignalPtr,
                          blk: untyped
                        ): auto =
  let ctx {.inject.} = newSharedPtr(TaskCtxObj[T](signal: signal))
  try:
    dispatchTaskWrap[T](self, signal, blk)
  except CancelledError as exc:
    trace "Cancelling thread future!", exc = exc.msg
    ctx.setCancelled()
    raise exc
  finally:
    discard ctx[].signal.close()
    self.semaphore.release()


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

import os

proc queryTask[DB](
    ctx: TaskCtx[QResult],
    ds: DB,
    query: DbQuery[KeyId],
) {.gcsafe, nimcall.} =
  ## run query command
  executeTask(ctx):
    # we execute this all inside `executeTask`
    # so we need to return a final result
    let handleRes = ds.query(query)
    if handleRes.isErr():
      # set error and exit executeTask, which will fire final signal
      (?!QResult).err(handleRes.error())
    else:
      # otherwise manually an set empty ok result
      ctx[].res.ok (KeyId.none, DataBuffer(), )
      discard ctx[].signal.fireSync()

      withLock(ctx[].nextSignal[0]):
        wait(ctx[].nextSignal[1], ctx[].nextSignal[0])
      # if not nextSignal.waitSync(10.seconds).get():
      #   raise newException(DeadThreadDefect, "query task timeout; possible deadlock!")

      var handle = handleRes.get()
      for item in handle.iter():
        # wait for next request from async thread

        if ctx[].cancelled:
          # cancel iter, then run next cycle so it'll finish and close
          handle.cancel = true
          continue
        else:
          ctx[].res = item.mapErr() do(exc: ref CatchableError) -> ThreadResErr:
            exc

          discard ctx[].signal.fireSync()

          # discard nextSignal.waitSync().get()
          withLock(ctx[].nextSignal[0]):
            wait(ctx[].nextSignal[1], ctx[].nextSignal[0])

      # set final result
      (?!QResult).ok((KeyId.none, DataBuffer()))

method query*(self: ThreadDatastore,
              q: Query
              ): Future[?!QueryIter] {.async.} =
  ## performs async query
  ## keeps one thread running queryTask until finished 
  ## 
  await self.semaphore.acquire()
  without signal =? acquireSignal(), err:
    return failure err
  # without nextSignal =? acquireSignal(), err:
  #   return failure err
  let ctx {.inject.} = newSharedPtr(TaskCtxObj[QResult](signal: signal))
  ctx[].nextSignal[0].initLock()
  ctx[].nextSignal[1].initCond()

  try:
    let query = dbQuery(
      key= KeyId.new q.key.id(),
      value=q.value, limit=q.limit, offset=q.offset, sort=q.sort)

    # setup initial queryTask
    dispatchTaskWrap[DbQueryResponse[KeyId, DataBuffer]](self, signal):
      self.tp.spawn queryTask(ctx, ds, query)
    withLock(ctx[].nextSignal[0]):
      signal(ctx[].nextSignal[1])

    var
      lock = newAsyncLock() # serialize querying under threads
      iter = QueryIter.new()

    proc next(): Future[?!QueryResponse] {.async.} =
      let ctx = ctx
      try:
        trace "About to query"
        if lock.locked:
          return failure (ref DatastoreError)(msg: "Should always await query features")
        if iter.finished == true:
          return failure (ref QueryEndedError)(msg: "Calling next on a finished query!")

        await wait(ctx[].signal)

        if not ctx[].running:
          iter.finished = true

        defer:
          withLock(ctx[].nextSignal[0]):
            signal(ctx[].nextSignal[1])

        if ctx[].res.isErr():
          return err(ctx[].res.error())
        else:
          let qres = ctx[].res.get()
          let key = qres.key.map(proc (k: KeyId): Key = k.toKey())
          let data = qres.data.toSeq()
          return (?!QueryResponse).ok((key: key, data: data))
      except CancelledError as exc:
        trace "Cancelling thread future!", exc = exc.msg
        ctx.setCancelled()
        discard ctx[].signal.close()
        # discard nextSignal.close()
        self.semaphore.release()
        raise exc

    iter.next = next
    return success iter
  except CancelledError as exc:
    trace "Cancelling thread future!", exc = exc.msg
    discard signal.close()
    # discard nextSignal.close()
    self.semaphore.release()
    raise exc

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
