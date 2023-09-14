import std/atomics
import std/locks

type
  Semaphore* = object
    count: int
    size: int
    lock {.align: 64.}: Lock
    cond: Cond

func `=`*(dst: var Semaphore, src: Semaphore) {.error: "A semaphore cannot be copied".}
func `=sink`*(dst: var Semaphore, src: Semaphore) {.error: "An semaphore cannot be moved".}

proc init*(_: type Semaphore, count: uint): Semaphore =
  var
    lock: Lock
    cond: Cond

  lock.initLock()
  cond.initCond()

  Semaphore(count: count.int, size: count.int, lock: lock, cond: cond)

proc `=destroy`*(self: var Semaphore) =
  self.lock.deinitLock()
  self.cond.deinitCond()

proc count*(self: var Semaphore): int =
  self.count

proc size*(self: var Semaphore): int =
  self.size

proc acquire*(self: var Semaphore) {.inline.} =
  self.lock.acquire()
  while self.count <= 0:
    self.cond.wait(self.lock)

  self.count -= 1
  self.lock.release()

proc release*(self: var Semaphore) {.inline.} =
  self.lock.acquire()
  if self.count <= 0:
    self.count += 1
    self.cond.signal()
  self.lock.release()

  doAssert not (self.count > self.size),
    "Semaphore count is greather than size: " & $self.size & " count is: " & $self.count

template withSemaphore*(self: var Semaphore, body: untyped) =
  self.acquire()
  try:
    body
  finally:
    self.release()
