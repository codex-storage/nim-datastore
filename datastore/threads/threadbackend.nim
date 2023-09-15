import pkg/chronos
import pkg/chronos/threadsync
import pkg/questionable
import pkg/questionable/results
import stew/results
import pkg/upraises
import pkg/taskpools

import ./sharedptr
import ../key
import ../query
import ./datastore
import ./databuffer
import ./threadresults

# import pretty
export key, query, sharedptr, databuffer
export threadresults

push: {.upraises: [].}

## Design Notes
## ============
## This is the threaded backend for `threadproxyds.nim`. It requires
## a `TResult[T]` to already be allocated, and uses it to "return" 
## the data. The `taskpools` worker uses `TResult[T]` to signal
## Chronos that the associated future is ready. Then the future on the
## `threadproxyds` frontend can read the results from `TResult[T]`.
##
## `TResult[T]` handles the shared memory aspect so each threaded
## task here can rely on having the memory until it finishes it's
## work. Even if the future exits early, the thread workers won't
## need to worry about using free-ed memory.
##
## The `FlowVar[T]` in `taskpools` isn't really suitable because
## we want to use Chronos's `ThreadSignalPtr` notification mechanism.
## Likewise the signaling mechanism in `taskpools` isn't suitable
## for the same reason. We need to notify Chronos when our work
## is done.
##
##
## Potential Issues
## ================
## One issue still outstanding with this setup and using a 
## ThreadSignalPtr pool is if `threadproxyds` frontend called
## `tresult.release()` early due to a `myFuture.cancel()` scenario.
## In this case the task here would then fire `tresult[].signal.fireAsync()`.
## If another `threadproxyds` had gotten that same ThreadSignalPtr it'd
## potentially get the signal. In this case the `TResult` would still be empty.
## It shouldn't corrupt memory, but the `threadproxyds` TResult would return "empty".
##
## Note I'm not sure if using ThreadSignalPtr's directly and closing them would work
## as File sockets are just int's on Linux/Mac and can be racey. It may be possible
## that if both sides don't `close` the AsyncFD that are used, you'd just get events
## from another pipe/socketpair which shares the same AsyncFD int's. Probably a solution
## to this but needs some more consideration.
##

type
  ThreadDatastore* = object
    tp*: Taskpool
    ds*: Datastore

  ThreadDatastorePtr* = SharedPtr[ThreadDatastore]

  QueryIterStore* = object
    it*: QueryIter
  QueryIterPtr* = SharedPtr[QueryIterStore]

proc hasTask*(
  sig: SharedSignal,
  ret: TResult[bool],
  tds: ThreadDatastorePtr,
  kb: KeyBuffer,
) =

  let key = kb.toKey()

  try:
    let res = waitFor tds[].ds.has(key)
    if res.isErr:
      ret.failure(res.error())
    else:
      ret.success(res.get())
    discard sig.fireSync()
  except CatchableError as err:
    ret.failure(err)

proc deleteTask*(
  sig: SharedSignal,
  ret: TResult[void],
  tds: ThreadDatastorePtr,
  kb: KeyBuffer,
) =

  let key = kb.toKey()

  let res = (waitFor tds[].ds.delete(key)).catch
  # print "thrbackend: putTask: fire", ret[].signal.fireSync().get()
  if res.isErr:
    ret.failure(res.error())
  else:
    ret.success()

  discard sig.fireSync()

proc getTask*(
  sig: SharedSignal,
  ret: TResult[DataBuffer],
  tds: ThreadDatastorePtr,
  kb: KeyBuffer,
) =
  echo "\n"
  echoed "getTask: ", $getThreadId()
  let key = kb.toKey()
  echoed "getTask: key: ", $key
  try:
    let res = waitFor tds[].ds.get(key)
    if res.isErr:
      ret.failure(res.error())
    else:
      let db = DataBuffer.new res.get()
      ret.success(db)

    discard sig.fireSync()
  except CatchableError as err:
    ret.failure(err)

import std/os

proc putTask*(
  sig: SharedSignal,
  ret: TResult[void],
  tds: ThreadDatastorePtr,
  kb: KeyBuffer,
  db: DataBuffer,
) =

  # os.sleep(1_000)
  # var ret = ret
  echo "\n"
  echoed "putTask: ", $getThreadId()
  echoed "putTask:kb: ", kb.toString
  echoed "putTask:db: ", db.toString

  let key = kb.toKey()

  let data = db.toSeq(byte)
  let res = (waitFor tds[].ds.put(key, data)).catch
  # print "thrbackend: putTask: fire", ret[].signal.fireSync().get()
  if res.isErr:
    ret.failure(res.error())
  else:
    ret.success()

  discard sig.fireSync()
  sig.decr()
  echoed "putTask: FINISH\n"

proc queryTask*(
  sig: SharedSignal,
  ret: TResult[QueryResponseBuffer],
  tds: ThreadDatastorePtr,
  qiter: QueryIterPtr,
) =

  try:
    # os.sleep(100)
    without res =? waitFor(qiter[].it.next()), err:
      ret.failure(err)

    let qrb = res.toBuffer()
    # print "queryTask: ", " res: ", res

    ret.success(qrb)
    # print "queryTask: ", " qrb:key: ", ret[].results.get().key.toString()
    # print "queryTask: ", " qrb:data: ", ret[].results.get().data.toString()

  except Exception as exc:
    ret.failure(exc)

  discard sig.fireSync()

proc query*(
  sig: SharedSignal,
  ret: TResult[QueryResponseBuffer],
  tds: ThreadDatastorePtr,
  qiter: QueryIterPtr,
) =
  tds[].tp.spawn queryTask(sig, ret, tds, qiter)