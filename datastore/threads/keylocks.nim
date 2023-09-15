import pkg/chronos/threadsync
import pkg/chronos
import std/locks
import std/sets

import ./databuffer
import ./sharedptr

export databuffer
export sharedptr
export threadsync

const
  KeyLocksSize {.intdefine.} = 100
  KeyLocksRetries {.intdefine.} = 1000

var
  keyLocksLock: Lock
  keyLocksUsed: HashSet[KeyBuffer]

type
  KeyLock* = object
    ## TODO: anything here?

proc initKeyLocks() =
  keyLocksLock.initLock()
  keyLocksUsed = initHashSet[KeyBuffer](2*KeyLocksSize) ## avoid re-allocating this

initKeyLocks()

proc acquireKeyLock*(key: KeyBuffer): Future[KeyLock] {.async, raises: [].} =
  ## Simple locking table for Datastore keys with async backpressure
  ## 
  {.cast(gcsafe).}:
    var cnt = KeyLocksRetries
    while cnt > 0:
      cnt.dec()
      keyLocksLock.acquire()
      try:
        if key notin keyLocksUsed:
          keyLocksUsed.incl(key)
          return 
      except KeyError:
        discard
      finally:
        keyLocksLock.release()
      # echo "wait:KeyLocksUsed: "
      await sleepAsync(1.milliseconds)
    raise newException(DeadThreadDefect, "reached limit trying to acquire a KeyBuffer")

proc release*(key: KeyBuffer) {.raises: [].} =
  ## Release KeyBuffer back to the pool in a thread-safe way.
  {.cast(gcsafe).}:
    withLock(keyLocksLock):
      keyLocksUsed.excl(key)
      # echo "free:KeyLocksUsed:size: ", KeyLocksUsed.len()

