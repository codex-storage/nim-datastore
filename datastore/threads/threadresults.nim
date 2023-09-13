
import pkg/chronos/threadsync
import pkg/chronos
import std/locks
import std/sets

import ./sharedptr
import ./databuffer
import ./threadsignalpool

export threadsignalpool
export databuffer
export sharedptr
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
    sig*: SharedSignal
    results*: Result[T, CatchableErrorBuffer]

  TResult*[T] = SharedPtr[ThreadResult[T]] ##\
    ## SharedPtr that allocates a shared buffer and keeps the 
    ## memory allocated until all references to it are gone.
    ## 
    ## Since ThreadResult is a plain object, its lifetime can be
    ## tied to that of an async proc. In this case it could be
    ## freed before the other background thread is finished.
    ## 
    ## For example, `myFuture.cancel()` can end an async proc early. 
    ## If the ThreadResult was stored in the async's memory then it'd
    ## be free'ed along with the rest of the async env. This would
    ## result in likely memory corruption (use-after-free).
    ## 

proc threadSafeType*[T: ThreadSafeTypes](tp: typedesc[T]) =
  ## Used to explicitly mark a type as threadsafe. It's checked
  ## at compile time in `newThreadResult`. 
  ## 
  ## Warning! Only non-GC types should be used!
  discard

proc newThreadResult*[T](
    tp: typedesc[T]
): Future[TResult[T]] {.async.} =
  ## Creates a new TResult including getting
  ## a new ThreadSignalPtr from the pool.
  ## 
  mixin threadSafeType
  when not compiles(threadSafeType):
    {.error: "only thread safe types can be used".}

  let res = newSharedPtr(ThreadResult[T])
  res[].sig = await SharedSignal.new()
  res

# proc release*[T](res: var TResult[T]) {.raises: [].} =
#   ## release TResult and it's ThreadSignal
#   # res[].signal.release()
#   sharedptr.release(res)

proc wait*[T](res: TResult[T]): Future[void] =
  res[].sig.wait()
proc fireSync*[T](res: TResult[T]): Result[bool, string] =
  res[].sig.fireSync()

proc success*[T](ret: TResult[T], value: T) =
  ## convenience wrapper for `TResult` to replicate
  ## normal questionable api
  ret[].results.ok(value)

proc success*[T: void](ret: TResult[T]) =
  ## convenience wrapper for `TResult` to replicate
  ## normal questionable api
  ret[].results.ok()

proc failure*[T](ret: TResult[T], exc: ref Exception) =
  ## convenience wrapper for `TResult` to replicate
  ## normal questionable api
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
    else:
      result.ok(ret[].results.get())
  else:
    let exc: ref CatchableError = ret[].results.error().toCatchable()
    result.err(exc)