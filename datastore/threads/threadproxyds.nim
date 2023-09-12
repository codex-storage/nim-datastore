
when not compileOption("threads"):
  {.error: "This module requires --threads:on compilation flag".}

import pkg/upraises

push: {.upraises: [].}

import std/atomics
import std/strutils

import pkg/chronos
import pkg/chronos/threadsync
import pkg/questionable
import pkg/questionable/results
import pkg/stew/ptrops
import pkg/taskpools
import pkg/threading/smartptrs
import pkg/stew/byteutils

import ../key
import ../query
import ../datastore

import ./asyncsemaphore
import ./databuffer

type
  ThreadTypes = void | bool | SomeInteger | DataBuffer
  ThreadResult[T: ThreadTypes] = Result[T, DataBuffer]

  TaskCtx[T: ThreadTypes] = object
    ds: ptr Datastore
    res: ptr ThreadResult[T]
    signal: ThreadSignalPtr

  ThreadDatastore* = ref object of Datastore
    tp: Taskpool
    ds: Datastore
    semaphore: AsyncSemaphore
    tasks: seq[Future[void]]

template dispatchTask(self: ThreadDatastore, ctx: TaskCtx, runTask: proc): untyped =
  let
    fut = wait(ctx.signal)

  try:
    await self.semaphore.acquire()
    runTask()
    self.tasks.add(fut)
    await fut

    if ctx.res[].isErr:
      result = failure(ctx.res[].error())
  finally:
    discard ctx.signal.close()
    if (
      let idx = self.tasks.find(fut);
      idx != -1):
      self.tasks.del(idx)

    self.semaphore.release()

proc hasTask(
  ctx: ptr TaskCtx,
  key: ptr Key) =

  defer:
    discard ctx[].signal.fireSync()

  without res =? (waitFor ctx[].ds[].has(key[])).catch, error:
    ctx[].res[].err(error)
    return

  ctx[].res[].ok(res.get())

method has*(self: ThreadDatastore, key: Key): Future[?!bool] {.async.} =
  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure("Failed to create signal")

    res = ThreadResult[bool]()
    ctx = TaskCtx[bool](
      ds: addr self.ds,
      res: addr res,
      signal: signal)

  proc runTask() =
    self.tp.spawn hasTask(addr ctx, unsafeAddr key)

  self.dispatchTask(ctx, runTask)
  return success(res.get())

proc delTask(ctx: ptr TaskCtx, key: ptr Key) =
  defer:
    discard ctx[].signal.fireSync()

  without res =? (waitFor ctx[].ds[].delete(key[])).catch, error:
    ctx[].res[].err(error)
    return

  ctx[].res[].ok()

method delete*(
  self: ThreadDatastore,
  key: Key): Future[?!void] {.async.} =

  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure("Failed to create signal")

    res = ThreadResult[void]()
    ctx = TaskCtx[void](
      ds: addr self.ds,
      res: addr res,
      signal: signal)

  proc runTask() =
    self.tp.spawn delTask(addr ctx, unsafeAddr key)

  self.dispatchTask(ctx, runTask)
  return success()

method delete*(self: ThreadDatastore, keys: seq[Key]): Future[?!void] {.async.} =
  for key in keys:
    if err =? (await self.delete(key)).errorOption:
      return failure err

  return success()

proc putTask(
  ctx: ptr TaskCtx,
  key: ptr Key,
  data: DataBuffer,
  len: int) =
  ## run put in a thread task
  ##

  defer:
    discard ctx[].signal.fireSync()

  without res =? (waitFor ctx[].ds[].put(key[], @data)).catch, error:
    ctx[].res[].err(error)
    return

  ctx[].res[].ok()

method put*(
  self: ThreadDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async.} =

  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure("Failed to create signal")

    res = ThreadResult[void]()
    ctx = TaskCtx[void](
      ds: addr self.ds,
      res: addr res,
      signal: signal)

  proc runTask() =
    self.tp.spawn putTask(
      addr ctx,
      unsafeAddr key,
      DataBuffer.new(data),
      data.len)

  self.dispatchTask(ctx, runTask)
  return success()

method put*(
  self: ThreadDatastore,
  batch: seq[BatchEntry]): Future[?!void] {.async.} =

  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err

  return success()

proc getTask(
  ctx: ptr TaskCtx,
  key: ptr Key) =
  ## Run get in a thread task
  ##

  defer:
    discard ctx[].signal.fireSync()

  without res =? (waitFor ctx[].ds[].get(key[])).catch, error:
    var err = error.msg
    ctx[].res[].err(error)
    return

  var
    data = res.get()

  ctx[].res[].ok(DataBuffer.new(data))

method get*(
  self: ThreadDatastore,
  key: Key): Future[?!seq[byte]] {.async.} =

  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure("Failed to create signal")

  var
    res = ThreadResult[DataBuffer]()
    ctx = TaskCtx[DataBuffer](
      ds: addr self.ds,
      res: addr res,
      signal: signal)

  proc runTask() =
    self.tp.spawn getTask(addr ctx, unsafeAddr key)

  self.dispatchTask(ctx, runTask)
  return success(@(res.get()))

method close*(self: ThreadDatastore): Future[?!void] {.async.} =
  for task in self.tasks:
    await task.cancelAndWait()

  await self.ds.close()

func new*(
  self: type ThreadDatastore,
  ds: Datastore,
  tp: Taskpool): ?!ThreadDatastore =
  doAssert tp.numThreads > 1, "ThreadDatastore requires at least 2 threads"

  success ThreadDatastore(
    tp: tp,
    ds: ds,
    semaphore: AsyncSemaphore.new(tp.numThreads - 1)) # one thread is needed for the task dispatcher
