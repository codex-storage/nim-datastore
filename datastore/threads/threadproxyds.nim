
when not compileOption("threads"):
  {.error: "This module requires --threads:on compilation flag".}

import pkg/upraises

push: {.upraises: [].}

import std/atomics

import pkg/chronos
import pkg/chronos/threadsync
import pkg/questionable
import pkg/questionable/results
import pkg/stew/ptrops
import pkg/taskpools

import ../key
import ../query
import ../datastore

import ./foreignbuffer

type
  ThreadResults = object
    ok: Atomic[bool]
    msg: ForeignBuff[char]

  TaskCtx = object
    ds: ptr Datastore
    res: ptr ThreadResults
    signal: ThreadSignalPtr

  ThreadDatastore* = ref object of Datastore
    tp*: Taskpool
    ds*: Datastore

proc success(self: var ThreadResults) {.inline.} =
  self.ok.store(true)

proc failure(self: var ThreadResults, msg: var string) {.inline.} =
  self.ok.store(false)
  self.msg.attach(msg.toOpenArray(0, msg.high))

proc hasTask(
  ctx: ptr TaskCtx,
  key: ptr Key,
  doesHave: ptr bool) =

  without res =? (waitFor ctx[].ds[].has(key[])).catch, error:
    ctx[].res[].failure(error.msg)
    return

  doesHave[] = res.get()
  ctx[].res[].success()
  discard ctx[].signal.fireSync()

method has*(self: ThreadDatastore, key: Key): Future[?!bool] {.async.} =
  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure("Failed to create signal")

    key = key
    res = ThreadResults()
    ctx = TaskCtx(
      ds: addr self.ds,
      res: addr res,
      signal: signal)
    doesHave = false

  proc runTask() =
    self.tp.spawn hasTask(addr ctx, addr key, addr doesHave)

  try:
    runTask()
    await wait(ctx.signal)

    if ctx.res.ok.load() == false:
      return failure($(ctx.res.msg))

    return success(doesHave)
  finally:
    ctx.signal.close()

proc delTask(ctx: ptr TaskCtx, key: ptr Key) =
  without res =? (waitFor ctx[].ds[].delete(key[])).catch, error:
    ctx[].res[].failure(error.msg)
    return

  ctx[].res[].ok.store(true)
  discard ctx[].signal.fireSync()

method delete*(
  self: ThreadDatastore,
  key: Key): Future[?!void] {.async.} =

  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure("Failed to create signal")

    key = key
    res = ThreadResults()
    ctx = TaskCtx(
      ds: addr self.ds,
      res: addr res,
      signal: signal)

  proc runTask() =
    self.tp.spawn delTask(addr ctx, addr key)

  try:
    runTask()
    await wait(ctx.signal)

    if ctx.res.ok.load() == false:
      return failure("error")

    return success()
  finally:
    ctx.signal.close()

method delete*(self: ThreadDatastore, keys: seq[Key]): Future[?!void] {.async.} =
  for key in keys:
    if err =? (await self.delete(key)).errorOption:
      return failure err

  return success()

proc putTask(
  ctx: ptr TaskCtx,
  key: ptr Key,
  data: ptr UncheckedArray[byte],
  len: int) =
  ## run put in a thread task
  ##

  without res =? (waitFor ctx[].ds[].put(
      key[],
      @(toOpenArray(data, 0, len - 1)))).catch, error:
    ctx[].res[].failure(error.msg)
    return

  ctx[].res[].ok.store(true)
  discard ctx[].signal.fireSync()

method put*(
  self: ThreadDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async.} =

  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure("Failed to create signal")
    key = key
    data = data
    res = ThreadResults()
    ctx = TaskCtx(
      ds: addr self.ds,
      res: addr res,
      signal: signal)

  proc runTask() =
    self.tp.spawn putTask(
      addr ctx,
      addr key,
      makeUncheckedArray(baseAddr data),
      data.len)

  try:
    runTask()
    await wait(ctx.signal)
  finally:
    ctx.signal.close()

  if ctx.res[].ok.load() == false:
    return failure($(ctx.res[].msg))

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
  key: ptr Key,
  buf: ptr ForeignBuff[byte]) =
  ## Run get in a thread task
  ##

  without res =? (waitFor ctx[].ds[].get(key[])).catch, error:
    var err = error.msg
    ctx[].res[].failure(error.msg)
    return

  var
    data = res.get()

  buf[].attach(data)
  ctx[].res[].ok.store(res.isOk)
  discard ctx[].signal.fireSync()

method get*(
  self: ThreadDatastore,
  key: Key): Future[?!seq[byte]] {.async.} =

  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure("Failed to create signal")

    key = key
    buf = ForeignBuff[byte].init()
    res = ThreadResults()
    ctx = TaskCtx(
      ds: addr self.ds,
      res: addr res,
      signal: signal)

  proc runTask() =
    self.tp.spawn getTask(addr ctx, addr key, addr buf)

  try:
    runTask()
    await wait(ctx.signal)

    if ctx.res.ok.load() == false:
      return failure($(ctx.res[].msg))

    return success(buf.toSeq())
  finally:
    ctx.signal.close()

func new*(
  self: type ThreadDatastore,
  ds: Datastore,
  tp: Taskpool): ?!ThreadDatastore =
  success ThreadDatastore(tp: tp, ds: ds)
