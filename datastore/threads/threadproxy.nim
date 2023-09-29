
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
import std/isolation
import pkg/chronicles
import pkg/threading/smartptrs

import ../key
import ../query
import ./backend
# import ./fsbackend
# import ./sqlbackend

import ./asyncsemaphore
import ./databuffer
import ./threadresult

export threadresult, smartptrs, isolation, chronicles

logScope:
  topics = "datastore threadproxy"

type

  TaskCtxObj*[T: ThreadTypes] = object
    res*: ThreadResult[T]
    signal: ThreadSignalPtr
    running*: bool ## used to mark when a task worker is running
    cancelled*: bool ## used to cancel a task before it's started
    nextSignal: ThreadSignalPtr

  TaskCtx*[T] = SharedPtr[TaskCtxObj[T]]
    ## Task context object.
    ## This is a SharedPtr to make the query iter simpler

  ThreadProxy*[BT] = object
    tp: Taskpool
    backend*: BT
    semaphore: AsyncSemaphore # semaphore is used for backpressure \
                              # to avoid exhausting file descriptors

proc newTaskCtx*[T](tp: typedesc[T],
                    signal: ThreadSignalPtr,
                    nextSignal: ThreadSignalPtr = nil): TaskCtx[T] =
  newSharedPtr(TaskCtxObj[T](signal: signal, nextSignal: nextSignal))

proc setCancelled[T](ctx: TaskCtx[T]) =
    ctx[].cancelled = true

proc setRunning[T](ctx: TaskCtx[T]): bool =
    if ctx[].cancelled:
      return false
    ctx[].running = true
    return true
proc setDone[T](ctx: TaskCtx[T]) =
    ctx[].running = false

proc acquireSignal(): ?!ThreadSignalPtr =
  # echo "signal:OPEN!"
  let signal = ThreadSignalPtr.new()
  if signal.isErr():
    failure (ref CatchableError)(msg: "failed to aquire ThreadSignalPtr: " & signal.error())
  else:
    success signal.get()

template executeTask*[T](ctx: TaskCtx[T], blk: untyped) =
  ## executes a task on a thread work and handles cleanup after cancels/errors
  ## 
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
    discard ctx[].signal.fireSync()

template dispatchTaskWrap[BT](self: ThreadProxy[BT],
                              signal: ThreadSignalPtr,
                              blk: untyped
                              ): auto =
  var ds {.used, inject.} = self.backend
  proc runTask() =
    `blk`
  runTask()
  await wait(ctx[].signal)

template dispatchTask*[BT](self: ThreadProxy[BT],
                          signal: ThreadSignalPtr,
                          blk: untyped
                          ): auto =
  ## handles dispatching a task from an async context
  ## `blk` is the actions, it has `ctx` and `ds` variables in scope.
  ## note that `ds` is a generic
  try:
    dispatchTaskWrap[BT](self, signal, blk)
  except CancelledError as exc:
    trace "Cancelling thread future!", exc = exc.msg
    ctx.setCancelled()
    raise exc
  except CatchableError as exc:
    ctx.setCancelled()
    raise exc
  finally:
    discard ctx[].signal.close()
    self.semaphore.release()

proc hasTask[T, DB](ctx: TaskCtx[T], ds: DB, key: KeyId) {.gcsafe.} =
  ## run backend command
  mixin has
  executeTask(ctx):
    has(ds, key)

proc has*[BT](self: ThreadProxy[BT],
                key: Key): Future[?!bool] {.async.} =
  await self.semaphore.acquire()
  let signal = acquireSignal().get()
  # without signal =? acquireSignal(), err:
  #   return failure err

  let ctx = newTaskCtx(bool, signal=signal)
  dispatchTask(self, signal):
    let key = KeyId.new key.id()
    self.tp.spawn hasTask(ctx, ds, key)
  return ctx[].res.toRes(v => v)

proc deleteTask[T, DB](ctx: TaskCtx[T], ds: DB;
                       key: KeyId) {.gcsafe.} =
  ## run backend command
  mixin delete
  executeTask(ctx):
    delete(ds, key)

proc delete*[BT](self: ThreadProxy[BT],
               key: Key): Future[?!void] {.async.} =
  ## delete key
  await self.semaphore.acquire()
  let signal = acquireSignal().get()
  # without signal =? acquireSignal(), err:
  #   return failure err

  let ctx = newTaskCtx(void, signal=signal)
  dispatchTask(self, signal):
    let key = KeyId.new key.id()
    self.tp.spawn deleteTask(ctx, ds, key)

  return ctx[].res.toRes()

proc delete*[BT](self: ThreadProxy[BT],
               keys: seq[Key]): Future[?!void] {.async.} =
  ## delete batch
  for key in keys:
    if err =? (await self.delete(key)).errorOption:
      return failure err

  return success()


proc putTask[T, DB](ctx: TaskCtx[T], ds: DB;
                    key: KeyId,
                    data: DataBuffer) {.gcsafe, nimcall.} =
  mixin put
  executeTask(ctx):
    put(ds, key, data)

proc put*[BT](self: ThreadProxy[BT],
            key: Key,
            data: seq[byte]): Future[?!void] {.async.} =
  ## put key with data
  await self.semaphore.acquire()
  let signal = acquireSignal().get()
  # without signal =? acquireSignal(), err:
  #   return failure err

  let ctx = newTaskCtx(void, signal=signal)
  dispatchTask(self, signal):
    let key = KeyId.new key.id()
    let data = DataBuffer.new data
    self.tp.spawn putTask(ctx, ds, key, data)

  return ctx[].res.toRes()

proc put*[E, DB](self: ThreadProxy[DB],
                 batch: seq[E]): Future[?!void] {.async.} =
  ## put batch data
  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err
  
  return success()


proc getTask[DB](ctx: TaskCtx[DataBuffer], ds: DB;
                 key: KeyId) {.gcsafe, nimcall.} =
  ## run backend command
  mixin get
  executeTask(ctx):
    let res = get(ds, key)
    res

proc get*[BT](self: ThreadProxy[BT],
                key: Key,
                ): Future[?!seq[byte]] {.async.} =
  await self.semaphore.acquire()
  let signal = acquireSignal().get()
  # without signal =? acquireSignal(), err:
  #   return failure err

  let ctx = newTaskCtx(DataBuffer, signal=signal)
  dispatchTask(self, signal):
    let key = KeyId.new key.id()
    self.tp.spawn getTask(ctx, ds, key)

  return ctx[].res.toRes(v => v.toSeq())

proc close*[BT](self: ThreadProxy[BT]): Future[?!void] {.async.} =
  await self.semaphore.closeAll()
  self.backend.close()

type
  QResult = DbQueryResponse[KeyId, DataBuffer]

proc queryTask[DB](
    ctx: TaskCtx[QResult],
    ds: DB,
    query: DbQuery[KeyId],
) =
  ## run query command
  mixin queryIter
  executeTask(ctx):
    # we execute this all inside `executeTask`
    # so we need to return a final result
    let handleRes = query(ds, query)
    static:
      echo "HANDLE_RES: ", typeof(handleRes)
    if handleRes.isErr():
      # set error and exit executeTask, which will fire final signal
      (?!QResult).err(handleRes.error())
    else:
      # otherwise manually an set empty ok result
      ctx[].res.ok (KeyId.none, DataBuffer(), )
      discard ctx[].signal.fireSync()
      if not ctx[].nextSignal.waitSync(10.seconds).get():
        raise newException(DeadThreadDefect, "queryTask timed out")

      var handle = handleRes.get()
      static:
        echo "HANDLE: ", typeof(handle)
      for item in handle.queryIter():
        # wait for next request from async thread

        if ctx[].cancelled:
          # cancel iter, then run next cycle so it'll finish and close
          handle.cancel = true
          continue
        else:
          ctx[].res = item.mapErr() do(exc: ref CatchableError) -> ThreadResErr:
            exc

          discard ctx[].signal.fireSync()
          if not ctx[].nextSignal.waitSync(10.seconds).get():
            raise newException(DeadThreadDefect, "queryTask timed out")

      # set final result
      (?!QResult).ok((KeyId.none, DataBuffer()))

proc query*[BT](self: ThreadProxy[BT],
              q: Query
              ): Future[?!QueryIter] {.async.} =
  ## performs async query
  ## keeps one thread running queryTask until finished 
  ## 
  await self.semaphore.acquire()
  let signal = acquireSignal().get()
  # without signal =? acquireSignal(), err:
  #   return failure err
  let nextSignal = acquireSignal().get()
  # without nextSignal =? acquireSignal(), err:
  #   return failure err
  let ctx = newTaskCtx(QResult, signal=signal, nextSignal=nextSignal)

  proc iterDispose() {.async.} =
    ctx.setCancelled()
    await ctx[].nextSignal.fire()
    discard ctx[].signal.close()
    discard ctx[].nextSignal.close()
    self.semaphore.release()

  try:
    let query = dbQuery(
      key= KeyId.new q.key.id(),
      value=q.value, limit=q.limit, offset=q.offset, sort=q.sort)

    # setup initial queryTask
    dispatchTaskWrap(self, signal):
      self.tp.spawn queryTask(ctx, ds, query)
    await ctx[].nextSignal.fire()

    var lock = newAsyncLock() # serialize querying under threads
    var iter = QueryIter.new()
    iter.dispose = proc (): Future[?!void] {.async.} =
      iterDispose()
      success()

    iter.next = proc(): Future[?!QueryResponse] {.async.} =
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
          await ctx[].nextSignal.fire()

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
        await iterDispose() # todo: is this valid?
        raise exc

    return success iter
  except CancelledError as exc:
    trace "Cancelling thread future!", exc = exc.msg
    ctx.setCancelled()
    await iterDispose()
    raise exc

proc new*[DB](self: type ThreadProxy,
          db: DB,
          withLocks = static false,
          tp: Taskpool
          ): ?!ThreadProxy[DB] =
  doAssert tp.numThreads > 1, "ThreadProxy requires at least 2 threads"

  success ThreadProxy[DB](
    tp: tp,
    backend: db,
    semaphore: AsyncSemaphore.new(tp.numThreads - 1)
  )