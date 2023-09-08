
import threading/smartptrs

import pkg/upraises
push: {.upraises: [].}

import pkg/chronos/threadsync

import ./foreignbuffer

type
  CatchableErrorBuffer = ForeignBuffer[ref CatchableError]

  ThreadResult*[T] = object
    signal*: ThreadSignalPtr
    results*: Result[T, CatchableErrorBuffer]

  TResult*[T] = SharedPtr[ThreadResult[T]]

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
    # elif S is QueryResponse:
    #   result.ok(ret[].results.get().toQueryResponse())
    else:
      result.ok(ret[].results.get())
  else:
    let exc: ref CatchableError = ret[].results.error().toCatchable()
    result.err(exc)

proc new*[T](
  self: type ThreadResult,
  tp: typedesc[T]): Result[TResult[T], ref CatchableError] =
  ## Create a new ThreadResult for type T
  ##

  let
    res = newSharedPtr(ThreadResult[T])
    signal = ThreadSignalPtr.new()

  if signal.isErr:
    return err((ref CatchableError)(msg: signal.error()))
  else:
    res[].signal = signal.get()

  ok res
