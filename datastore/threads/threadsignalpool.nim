import pkg/chronos/threadsync
import pkg/threading/smartptrs
import pkg/chronos
import std/locks
import std/sets

import ./databuffer

export databuffer
export smartptrs
export threadsync

const
  SignalPoolSize {.intdefine.} = 1024
  SignalPoolRetries {.intdefine.} = 100

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
  ## This could be put onto its own thread and use it's own set ThreadSignalPtr
  ## to become a true "resource pool".  
  ## For now the sleepAsync should prove if this setup is useful
  ## or not before going into that effort.
  ## 
  ## TLDR: if all ThreadSignalPtr's are used up, this will 
  ##        repetedly call `sleepAsync` deferring whatever request
  ##        is until more ThreadSignalPtr's are available. This
  ##        design isn't particularly fair, but should let us handle
  ##        periods of overloads with lots of requests in flight.
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
          # echo "get:signalPoolUsed:size: ", signalPoolUsed.len()
          return res
      except KeyError:
        discard
      finally:
        signalPoolLock.release()
      # echo "wait:signalPoolUsed: "
      await sleepAsync(10.milliseconds)
    raise newException(DeadThreadDefect, "reached limit trying to acquire a ThreadSignalPtr")

proc release*(sig: ThreadSignalPtr) {.raises: [].} =
  ## Release ThreadSignalPtr back to the pool in a thread-safe way.
  {.cast(gcsafe).}:
    withLock(signalPoolLock):
      signalPoolUsed.excl(sig)
      signalPoolFree.incl(sig)
      # echo "free:signalPoolUsed:size: ", signalPoolUsed.len()


type
  SharedSignalPtr* = object
    cnt: ptr int
    buf*: ThreadSignalPtr


proc `$`*(data: SharedSignalPtr): string =
  if data.buf.isNil:
    result = "nil"
  else:
    result = data.buf.pointer.repr

proc `=destroy`*(x: var SharedSignalPtr) =
  if x.buf != nil and x.cnt != nil:
    let res = atomicSubFetch(x.cnt, 1, ATOMIC_ACQUIRE)
    if res == 0:
      # for i in 0..<x.len: `=destroy`(x.data[i])
      echo "SIGNAL: FREE: ", repr x.buf.pointer, " ", x.cnt[]
      deallocShared(x.buf)
      deallocShared(x.cnt)
    else:
      echo "SIGNAL: decr: ", repr x.buf.pointer, " ", x.cnt[]

proc `=copy`*(a: var SharedSignalPtr; b: SharedSignalPtr) =
  # do nothing for self-assignments:
  if a.buf == b.buf: return
  `=destroy`(a)
  discard atomicAddFetch(b.cnt, 1, ATOMIC_RELAXED)
  a.buf = b.buf
  a.cnt = b.cnt
  echo "SIGNAL: Copy: repr: ", b.cnt[],
          " ", repr a.buf.pointer, 
          " ", repr b.buf.pointer

proc `incr`*(a: SharedSignalPtr) =
  echo "SIGNAL: incr: ", atomicAddFetch(a.cnt, 1, ATOMIC_RELAXED)

proc newSharedSignalPtr*(): Future[SharedSignalPtr] {.async, raises: [].} =
  result.cnt = cast[ptr int](allocShared0(sizeof(result.cnt)))
  result.buf = await getThreadSignal()

template fireSync*(sig: SharedSignalPtr): untyped =
  let ts: ThreadSignalPtr = sig.buf
  ts.fireSync()

template wait*(sig: SharedSignalPtr): untyped =
  let ts: ThreadSignalPtr = sig.buf
  await ts.wait()
