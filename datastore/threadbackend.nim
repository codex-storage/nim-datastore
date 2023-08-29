import pkg/chronos
import pkg/chronos/threadsync
import pkg/questionable
import pkg/questionable/results
import stew/results
import pkg/upraises
import pkg/taskpools

import ./key
import ./query
import ./datastore
import ./databuffer
import threading/smartptrs

import pretty

export key, query, smartptrs, databuffer

push: {.upraises: [].}

type

  ThreadResultKind* {.pure.} = enum
    NotReady
    Success
    Error

  ThreadResult*[T: DataBuffer | void | bool | ThreadDatastorePtr] = object
    signal*: ThreadSignalPtr
    case state*: ThreadResultKind
    of ThreadResultKind.NotReady:
      discard
    of ThreadResultKind.Success:
      value*: T
    of ThreadResultKind.Error:
      error*: CatchableErrorBuffer

  TResult*[T] = SharedPtr[ThreadResult[T]]

  ThreadDatastore* = object
    tp*: Taskpool
    ds*: Datastore

  ThreadDatastorePtr* = SharedPtr[ThreadDatastore]

proc newThreadResult*[T](
    tp: typedesc[T]
): Result[TResult[T], ref CatchableError] =
  let res = newSharedPtr(ThreadResult[T])
  let signal = ThreadSignalPtr.new()
  if signal.isErr:
    return err((ref CatchableError)(msg: signal.error()))
  else:
    res[].signal = signal.get()
  ok res

proc success*[T](ret: TResult[T], value: T) =
  {.cast(uncheckedAssign).}:
    ret[].state = ThreadResultKind.Success
    ret[].value = value

proc success*[T: void](ret: TResult[T]) =
  {.cast(uncheckedAssign).}:
    ret[].state = ThreadResultKind.Success

proc failure*[T](ret: TResult[T], err: ref CatchableError) =
  {.cast(uncheckedAssign).}:
    ret[].state = ThreadResultKind.Error
  ret[].error = err.toBuffer()

proc hasTask*(
  ret: TResult[bool],
  tds: ThreadDatastorePtr,
  kb: KeyBuffer,
) =
  without key =? kb.toKey(), err:
    ret.failure(err)

  try:
    let res = waitFor tds[].ds.has(key)
    if res.isErr:
      ret.failure(res.error())
    else:
      ret.success(res.get())
    discard ret[].signal.fireSync()
  except CatchableError as err:
    ret.failure(err)

proc has*(
  ret: TResult[bool],
  tds: ThreadDatastorePtr,
  key: Key,
) =
  let bkey = StringBuffer.new(key.id())
  tds[].tp.spawn hasTask(ret, tds, bkey)

proc getTask*(
  ret: TResult[DataBuffer],
  tds: ThreadDatastorePtr,
  kb: KeyBuffer,
) =
  without key =? kb.toKey(), err:
    ret[].state = Error
  try:
    let res = waitFor tds[].ds.get(key)
    if res.isErr:
      ret.failure(res.error())
    else:
      let db = DataBuffer.new res.get()
      ret.success(db)

    discard ret[].signal.fireSync()
  except CatchableError as err:
    ret.failure(err)

proc get*(
  ret: TResult[DataBuffer],
  tds: ThreadDatastorePtr,
  key: Key,
) =
  let bkey = StringBuffer.new(key.id())
  tds[].tp.spawn getTask(ret, tds, bkey)


proc putTask*(
  ret: TResult[void],
  tds: ThreadDatastorePtr,
  kb: KeyBuffer,
  db: DataBuffer,
) =

  without key =? kb.toKey(), err:
    ret[].state = Error

  let data = db.toSeq(byte)
  let res = (waitFor tds[].ds.put(key, data)).catch
  # print "thrbackend: putTask: fire", ret[].signal.fireSync().get()
  if res.isErr:
    ret.failure(res.error())
  else:
    ret.success()

  discard ret[].signal.fireSync()

proc put*(
  ret: TResult[void],
  tds: ThreadDatastorePtr,
  key: Key,
  data: seq[byte]
) =
  let bkey = StringBuffer.new(key.id())
  let bval = DataBuffer.new(data)

  tds[].tp.spawn putTask(ret, tds, bkey, bval)


proc deleteTask*(
  ret: TResult[void],
  tds: ThreadDatastorePtr,
  kb: KeyBuffer,
) =

  without key =? kb.toKey(), err:
    ret[].state = Error

  let res = (waitFor tds[].ds.delete(key)).catch
  # print "thrbackend: putTask: fire", ret[].signal.fireSync().get()
  if res.isErr:
    ret.failure(res.error())
  else:
    ret.success()

  discard ret[].signal.fireSync()

proc delete*(
  ret: TResult[void],
  tds: ThreadDatastorePtr,
  key: Key,
) =
  let bkey = StringBuffer.new(key.id())
  tds[].tp.spawn deleteTask(ret, tds, bkey)
