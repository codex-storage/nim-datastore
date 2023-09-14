
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
import pkg/stew/byteutils

import ../key
import ../query
import ../datastore

import ./databuffer

type
  ThreadTypes = void | bool | SomeInteger | DataBuffer | tuple
  ThreadResult[T: ThreadTypes] = Result[T, DataBuffer]

  TaskCtx[T: ThreadTypes] = object
    ds: ptr Datastore
    res: ptr ThreadResult[T]
    signal: ThreadSignalPtr

  ThreadDatastore* = ref object of Datastore
    tp: Taskpool
    ds: Datastore
    tasks: seq[Future[void]]

template dispatchTask(
  self: ThreadDatastore,
  ctx: TaskCtx,
  runTask: proc): untyped =

  let
    fut = wait(ctx.signal)

  try:
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

proc hasTask(
  ctx: ptr TaskCtx,
  key: ptr Key) =

  defer:
    discard ctx[].signal.fireSync()

  without ret =?
    (waitFor ctx[].ds[].has(key[])).catch and res =? ret, error:
    ctx[].res[].err(error)
    return

  ctx[].res[].ok(res)

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

  without res =?
    (waitFor ctx[].ds[].get(key[])).catch and data =? res, error:
    ctx[].res[].err(error)
    return

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

proc queryTask(
  ctx: ptr TaskCtx,
  iter: ptr QueryIter) =

  defer:
    discard ctx[].signal.fireSync()

  without ret =? (waitFor iter[].next()).catch and res =? ret, error:
    ctx[].res[].err(error)
    return

  if res.key.isNone:
    ctx[].res[].ok((false, DataBuffer.new(), DataBuffer.new()))
    return

  var
    keyBuf = DataBuffer.new($(res.key.get()))
    dataBuf = DataBuffer.new(res.data)

  ctx[].res[].ok((true, keyBuf, dataBuf))

method query*(
  self: ThreadDatastore,
  query: Query): Future[?!QueryIter] {.async.} =

  without var childIter =? await self.ds.query(query), error:
    return failure error

  var
    iter = QueryIter.init()

  let lock = newAsyncLock()
  proc next(): Future[?!QueryResponse] {.async.} =
    defer:
      if lock.locked:
        lock.release()

    if iter.finished == true:
      return failure (ref QueryEndedError)(msg: "Calling next on a finished query!")

    await lock.acquire()

    if iter.finished == true:
      return success (Key.none, EmptyBytes)

    var
      signal = ThreadSignalPtr.new().valueOr:
        return failure("Failed to create signal")

      res = ThreadResult[(bool, DataBuffer, DataBuffer)]()
      ctx = TaskCtx[(bool, DataBuffer, DataBuffer)](
        ds: addr self.ds,
        res: addr res,
        signal: signal)

    proc runTask() =
      self.tp.spawn queryTask(addr ctx, addr childIter)

    self.dispatchTask(ctx, runTask)
    if err =? res.errorOption:
      return failure err

    let (ok, key, data) = res.get()
    if not ok:
      iter.finished = true
      return success (Key.none, EmptyBytes)

    return success (Key.init($key).expect("should not fail").some, @(data))

  iter.next = next
  return success iter

func new*(
  self: type ThreadDatastore,
  ds: Datastore,
  tp: Taskpool): ?!ThreadDatastore =
  doAssert tp.numThreads > 1, "ThreadDatastore requires at least 2 threads"

  success ThreadDatastore(
    tp: tp,
    ds: ds)
