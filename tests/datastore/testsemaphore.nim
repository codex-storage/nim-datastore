import std/os
import std/osproc

import pkg/unittest2
import pkg/taskpools

import pkg/stew/ptrops

import pkg/datastore/threads/semaphore

suite "Test semaphore":

  test "Should work as a mutex/lock":
    var
      tp = TaskPool.new(countProcessors() * 2)
      lock = Semaphore.init(1) # mutex/lock
      resource = 1
      count = 0

    const numTasks = 1000

    proc task(lock: ptr Semaphore, resource: ptr int, count: ptr int) =
      lock[].acquire()
      resource[] -= 1
      doAssert resource[] == 0, "resource should be 0, but it's: " & $resource[]

      resource[] += 1
      doAssert resource[] == 1, "resource should be 1, but it's: " & $resource[]

      count[] += 1
      lock[].release()

    for i in 0..<numTasks:
      tp.spawn task(addr lock, addr resource, addr count)

    tp.syncAll()
    tp.shutdown()

    check: count == numTasks

  test "Should not exceed limit":
    var
      tp = TaskPool.new(countProcessors() * 2)
      lock = Semaphore.init(5)
      resource = 5
      count = 0

    const numTasks = 1000

    template testInRange(l, h, item: int) =
      doAssert item in l..h, "item should be in range [" & $l & ", " & $h & "], but it's: " & $item

    proc task(lock: ptr Semaphore, resource: ptr int, count: ptr int) =
      lock[].acquire()
      resource[] -= 1
      testInRange(1, 5, resource[])

      resource[] += 1
      testInRange(1, 5, resource[])

      count[] += 1
      lock[].release()

    for i in 0..<numTasks:
      tp.spawn task(addr lock, addr resource, addr count)

    tp.syncAll()
    tp.shutdown()

    check: count == numTasks
