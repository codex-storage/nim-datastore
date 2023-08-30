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

export key, query, smartptrs, databuffer

push: {.upraises: [].}

type
  ThreadSafeTypes* = DataBuffer | void | bool | ThreadDatastorePtr | QueryResponseBuffer
  ThreadResult*[T: ThreadSafeTypes] = object
    signal*: ThreadSignalPtr
    results*: Result[T, CatchableErrorBuffer]

  TResult*[T] = SharedPtr[ThreadResult[T]]

  ThreadDatastore* = object
    tp*: Taskpool
    ds*: Datastore

  ThreadDatastorePtr* = SharedPtr[ThreadDatastore]

  QueryIterStore* = object
    it*: QueryIter
  QueryIterPtr* = SharedPtr[QueryIterStore]

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
  ret[].results.ok(value)

proc success*[T: void](ret: TResult[T]) =
  ret[].results.ok()

proc failure*[T](ret: TResult[T], exc: ref Exception) =
  ret[].results.err(exc.toBuffer())

proc convert*[T, S](ret: TResult[T], tp: typedesc[S]): Result[S, ref CatchableError] =
  if ret[].results.isOk():
    when S is seq[byte]:
      result.ok(ret[].results.get().toSeq(byte))
    elif S is string:
      result.ok(ret[].results.get().toString())
    elif S is void:
      result.ok()
    elif S is QueryResponse:
      result.ok(ret[].results.get().toQueryResponse())
    else:
      result.ok(ret[].results.get())
  else:
    let exc: ref CatchableError = ret[].results.error().toCatchable()
    result.err(exc)

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
    ret.failure(err)
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
    ret.failure(err)

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
    ret.failure(err)

  let res = (waitFor tds[].ds.delete(key)).catch
  # print "thrbackend: putTask: fire", ret[].signal.fireSync().get()
  if res.isErr:
    ret.failure(res.error())
  else:
    ret.success()

  discard ret[].signal.fireSync()

import pretty

proc delete*(
  ret: TResult[void],
  tds: ThreadDatastorePtr,
  key: Key,
) =
  let bkey = StringBuffer.new(key.id())
  tds[].tp.spawn deleteTask(ret, tds, bkey)

import os

proc queryTask*(
  ret: TResult[QueryResponseBuffer],
  tds: ThreadDatastorePtr,
  qiter: QueryIterPtr,
) =

  try:
    os.sleep(100)
    without res =? waitFor(qiter[].it.next()), err:
      ret.failure(err)

    let qrb = res.toBuffer()
    # print "queryTask: ", " res: ", res

    ret.success(qrb)
    print "queryTask: ", " qrb:key: ", ret[].results.get().key.toString()
    print "queryTask: ", " qrb:data: ", ret[].results.get().data.toString()

  except Exception as exc:
    ret.failure(exc)

  discard ret[].signal.fireSync()

proc query*(
  ret: TResult[QueryResponseBuffer],
  tds: ThreadDatastorePtr,
  qiter: QueryIterPtr,
) =
  tds[].tp.spawn queryTask(ret, tds, qiter)
