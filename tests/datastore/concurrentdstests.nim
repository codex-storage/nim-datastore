import std/options
import std/sugar
import std/random
import std/sequtils

import pkg/asynctest
import pkg/chronos
import pkg/stew/endians2
import pkg/questionable
import pkg/questionable/results

import pkg/datastore/concurrentds

proc concurrentStoreTests*(
  ds: ConcurrentDatastore,
  key: Key) =

  randomize()

  let processCount = 100

  proc withRandDelay(op: Future[?!void]): Future[void] {.async.} =
    await sleepAsync(rand(processCount).millis)

    let errMsg = (await op).errorOption.map((err) => err.msg)

    require none(string) == errMsg

  proc incAsyncFn(maybeBytes: ?seq[byte]): Future[?seq[byte]] {.async.} =
    await sleepAsync(2.millis) # allows interleaving
    if bytes =? maybeBytes:
      let value = uint64.fromBytes(bytes)
      return some(@((value + 1).toBytes))
    else:
      return seq[byte].none

  test "unsafe increment - demo":
    # this test demonstrates non synchronized read-modify-write sequence
    (await ds.put(key, @(0.uint64.toBytes))).tryGet

    proc getIncAndPut(): Future[?!void] {.async.} =
      without bytes =? (await ds.get(key)), err:
        return failure(err)

      let value = uint64.fromBytes(bytes)
      await sleepAsync(2.millis) # allows interleaving

      if err =? (await ds.put(key, @((value + 1).toBytes))).errorOption:
        return failure(err)
      else:
        return success()

    let futs = newSeqWith(processCount, withRandDelay(getIncAndPut()))
    await allFutures(futs).wait(10.seconds)

    let finalValue = uint64.fromBytes((await ds.get(key)).tryGet)

    check finalValue.int < processCount

  test "safe increment":
    (await ds.put(key, @(0.uint64.toBytes))).tryGet

    let futs = newSeqWith(processCount, withRandDelay(ds.modify(key, incAsyncFn)))
    await allFutures(futs).wait(10.seconds)

    let finalValue = uint64.fromBytes((await ds.get(key)).tryGet)

    check finalValue.int == processCount

  test "should update value":
    (await ds.put(key, @((0.uint64).toBytes))).tryGet

    (await ds.modify(key, incAsyncFn)).tryGet

    let finalValue = uint64.fromBytes((await ds.get(key)).tryGet)

    check finalValue.int == 1

  test "should put value":
    (await ds.delete(key)).tryGet()

    (await ds.modify(key, (_: ?seq[byte]) => @(123.uint64.toBytes).some)).tryGet

    let finalValue = uint64.fromBytes((await ds.get(key)).tryGet)

    check finalValue.int == 123

  test "should delete value":
    let key = Key.init(Key.random).tryGet
    (await ds.put(key, @(0.uint64.toBytes))).tryGet

    (await ds.modify(key, (_: ?seq[byte]) => seq[byte].none)).tryGet

    let hasKey = (await ds.has(key)).tryGet

    check not hasKey
