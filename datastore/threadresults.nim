
import pkg/chronos/threadsync
import pkg/threading/smartptrs

import ./databuffer

export databuffer
export smartptrs
export threadsync

type
  ThreadSafeTypes* = DataBuffer | void | bool | SharedPtr ##\
    ## This is a whitelisting of types that can be used with ThreadResult below
    ## These types need to be thread safe with refc. That means no
    ## GC types.

  ThreadResult*[T] = object
    ## Encapsulates both the results from a thread but also the cross
    ## thread signaling mechanism. This makes it easier to keep them 
    ## together.
    signal*: ThreadSignalPtr
    results*: Result[T, CatchableErrorBuffer]

  TResult*[T] = SharedPtr[ThreadResult[T]] ##\
    ## SharedPtr that allocates a shared buffer and keeps the 
    ## memory allocated until all references to it are gone.
    ## 
    ## Since ThreadResult is a plain object, and if its lifetime is
    ## tied to that of an async proc or the thread-backend request
    ## it could be freed before the other thread is finished.
    ## 
    ## For example, `myFuture.cancel()` can end an async proc early. 
    ## If the ThreadResult was stored in the async's memory then it'd
    ## be free'ed along with the rest of the async env. This would
    ## result in likely memory corruption (use-after-free).

proc threadSafeType*[T: ThreadSafeTypes](tp: typedesc[T]) =
  discard

proc newThreadResult*[T](
    tp: typedesc[T]
): Result[TResult[T], ref CatchableError] =
  ## Creates a new TResult including allocating
  ## a new ThreadSignalPtr.
  ## 
  ## Since allocating the TSP can fail, this returns 
  ## a Result.
  mixin threadSafeType
  when not compiles(threadSafeType):
    {.error: "only thread safe types can be used".}

  let res = newSharedPtr(ThreadResult[T])
  let signal = ThreadSignalPtr.new()
  if signal.isErr:
    return err((ref CatchableError)(msg: signal.error()))
  else:
    res[].signal = signal.get()
  ok res

proc success*[T](ret: TResult[T], value: T) =
  ## convenience wrapper for `TResult` to make
  ## "returning" results easier
  ret[].results.ok(value)

proc success*[T: void](ret: TResult[T]) =
  ## convenience wrapper for `TResult` to make
  ## "returning" results easier
  ret[].results.ok()

proc failure*[T](ret: TResult[T], exc: ref Exception) =
  ## convenience wrapper for `TResult` to make
  ## "returning" results easier
  ret[].results.err(exc.toBuffer())

proc convert*[T, S](ret: TResult[T],
                    tp: typedesc[S]
                    ): Result[S, ref CatchableError] =
  ## convenience wrapper for `TResult` to make
  ## fetching results from `TResult` easier.
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