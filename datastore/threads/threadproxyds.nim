
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
  TaskRes = object
    ok: Atomic[bool]
    msg: ptr cstring

  TaskCtx = object
    ds: ptr Datastore
    res: TaskRes
    signal: ThreadSignalPtr

  ThreadDatastore* = ref object of Datastore
    tp*: Taskpool
    ds*: Datastore

proc hasTask(
  ctx: ptr TaskCtx,
  key: ptr Key,
  doesHave: ptr bool) =

  let
    res = (waitFor ctx[].ds[].has(key[])).catch

  if res.isErr:
    var
      err = cstring(res.error().msg)
    ctx[].res.msg = addr err
  else:
    ctx[].res.msg = nil
    doesHave[] = res.get().get()

  ctx[].res.ok.store(res.isOk)
  discard ctx[].signal.fireSync()

proc has*(
  self: ThreadDatastore,
  key: Key): Future[?!bool] {.async.} =

  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure("Failed to create signal")

    key = key
    ctx = TaskCtx(
      ds: addr self.ds,
      res: TaskRes(msg: nil),
      signal: signal)
    doesHave = false

  proc runTask() =
    self.tp.spawn hasTask(addr ctx, addr key, addr doesHave)

  try:
    runTask()
    await wait(ctx.signal)

    var data: bool
    if ctx.res.ok.load() == false:
      return failure("error")

    return success(doesHave)
  finally:
    ctx.signal.close()

proc delTask(ctx: ptr TaskCtx, key: ptr Key) =

  let
    res = (waitFor ctx[].ds[].delete(key[])).catch

  if res.isErr:
    var
      err = cstring(res.error().msg)
    ctx[].res.msg = addr err
  else:
    ctx[].res.msg = nil

  ctx[].res.ok.store(res.isOk)
  discard ctx[].signal.fireSync()

proc delete*(
  self: ThreadDatastore,
  key: Key): Future[?!void] {.async.} =

  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure("Failed to create signal")

    key = key
    ctx = TaskCtx(
      ds: addr self.ds,
      res: TaskRes(msg: nil),
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

proc putTask(
  ctx: ptr TaskCtx,
  key: ptr Key,
  data: ptr UncheckedArray[byte],
  len: int) =
  ## run put in a thread task
  ##

  let
    res = (waitFor ctx[].ds[].put(
      key[],
      @(toOpenArray(data, 0, len - 1)))).catch

  if res.isErr:
    var err = cstring(res.error().msg)
    ctx[].res.msg = addr err
  else:
    ctx[].res.msg = nil

  ctx[].res.ok.store(res.isOk)
  discard ctx[].signal.fireSync()

proc put*(
  self: ThreadDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async.} =

  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure("Failed to create signal")
    key = key
    data = data
    ctx = TaskCtx(
      ds: addr self.ds,
      res: TaskRes(msg: nil),
      signal: signal)

  proc runTask() =
    self.tp.spawn putTask(
      addr ctx,
      addr key, makeUncheckedArray(baseAddr data),
      data.len)

  try:
    runTask()
    await wait(ctx.signal)
  finally:
    ctx.signal.close()

  if ctx.res.ok.load() == false:
    return failure("error")

  return success()

proc getTask(
  ctx: ptr TaskCtx,
  key: ptr Key,
  buf: ptr ForeignBuff[byte]) =
  ## Run get in a thread task
  ##

  without res =? (waitFor ctx[].ds[].get(key[])).catch, error:
    var err = cstring(error.msg)
    ctx[].res.msg = addr err
    return

  var
    data = res.get()
    cell = protect(addr data)
  ctx[].res.msg = nil
  buf[].attach(
    makeUncheckedArray(baseAddr data), data.len, cell)

  ctx[].res.ok.store(res.isOk)
  discard ctx[].signal.fireSync()

proc get*(
  self: ThreadDatastore,
  key: Key): Future[?!seq[byte]] {.async.} =

  var
    signal = ThreadSignalPtr.new().valueOr:
        return failure("Failed to create signal")

    key = key
    buf = ForeignBuff[byte].init()
    ctx = TaskCtx(
      ds: addr self.ds,
      res: TaskRes(msg: nil),
      signal: signal)

  proc runTask() =
    self.tp.spawn getTask(addr ctx, addr key, addr buf)

  try:
    runTask()
    await wait(ctx.signal)

    if ctx.res.ok.load() == false:
      return failure("error")

    var data = @(toOpenArray(buf.get(), 0, buf.len - 1))
    return success(data)
  finally:
    ctx.signal.close()

proc new*(
  self: type ThreadDatastore,
  ds: Datastore,
  tp: Taskpool): ?!ThreadDatastore =
  success ThreadDatastore(tp: tp, ds: ds)
