
import pkg/chronos/threadsync
import pkg/threading/smartptrs
import pkg/chronos
import std/locks
import std/sets

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
    ## Important:
    ##    On `refc` that internal destructors for ThreadResult[T]
    ##    are *not* called. Effectively limiting this to 1 depth
    ##    of destructors. Hence the `threadSafeType` marker below.
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

const
  SignalPoolSize {.intdefine.} = 1024
  SignalPoolRetries {.intdefine.} = 1000

var
  signalPoolLock: Lock
  signalPoolFree: HashSet[ThreadSignalPtr]
  signalPoolUsed: HashSet[ThreadSignalPtr]

proc initSignalPool() =
  signalPoolLock.initLock()
  for i in 1..SignalPoolSize:
    let signal = ThreadSignalPtr.new().get()
    signalPoolFree.incl(signal)

initSignalPool()

proc getThreadSignal*(): Future[ThreadSignalPtr] {.async, raises: [].} =
  ## Get's a ThreadSignalPtr from the pool in a thread-safe way.
  ## 
  ## This provides a simple backpressue mechanism for the
  ## number of requests in flight (not for the file operations themselves).
  ## 
  ## This setup provides two benefits:
  ##  - backpressure on the number of disk IO requests
  ##  - prevents leaks in ThreadSignalPtr's from exhausting the 
  ##      processes IO descriptor limit, which results in bad
  ##      and unpredictable failure modes.
  ## 
  ## This could be put onto its own thread and use it's own set ThreadSignalPtr, 
  ## but the sleepAsync should prove if this is useful for not.
  ## 
  {.cast(gcsafe).}:
    var cnt = SignalPoolRetries
    while cnt > 0:
      cnt.dec()
      signalPoolLock.acquire()
      try:
        if signalPoolFree.len() > 0:
          let res = signalPoolFree.pop()
          signalPoolUsed.incl(res)
          echo "get:signalPoolUsed:size: ", signalPoolUsed.len()
          return res
      except KeyError:
        discard
      finally:
        signalPoolLock.release()
      echo "wait:signalPoolUsed: "
      await sleepAsync(10.milliseconds)
    raise newException(DeadThreadDefect, "reached limit trying to acquire a ThreadSignalPtr")

proc release*(sig: ThreadSignalPtr) {.raises: [].} =
  ## Release ThreadSignalPtr back to the pool in a thread-safe way.
  {.cast(gcsafe).}:
    withLock(signalPoolLock):
      signalPoolUsed.excl(sig)
      signalPoolFree.incl(sig)
      echo "free:signalPoolUsed:size: ", signalPoolUsed.len()

proc threadSafeType*[T: ThreadSafeTypes](tp: typedesc[T]) =
  ## Used to explicitly mark a type as threadsafe. It's checked
  ## at compile time in `newThreadResult`. 
  ## 
  ## Warning! Only non-GC types should be used!
  discard

proc newThreadResult*[T](
    tp: typedesc[T]
): Future[TResult[T]] {.async.} =
  ## Creates a new TResult including allocating
  ## a new ThreadSignalPtr.
  ## 
  ## Since allocating the TSP can fail, this returns 
  ## a Result.
  mixin threadSafeType
  when not compiles(threadSafeType):
    {.error: "only thread safe types can be used".}

  let res = newSharedPtr(ThreadResult[T])
  res[].signal = await getThreadSignal()
  res

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