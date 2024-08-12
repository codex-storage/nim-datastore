import std/options
import std/sugar
import std/random
import std/sequtils

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/endians2
import pkg/questionable
import pkg/questionable/results

import pkg/datastore

proc modifyTests*(
  ds: Datastore,
  key: Key,
  dsCount = 1) =

  randomize()

  let
    processCount = 100
    timeout = (1 + processCount div 10).seconds

  proc withRandDelay(op: Future[?!void]): Future[void] {.async: (raises: [Exception]).} =
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
    await allFutures(futs).wait(timeout)

    let finalValue = uint64.fromBytes((await ds.get(key)).tryGet)

    check finalValue.int < processCount

  test "safe increment":
    (await ds.put(key, @(0.uint64.toBytes))).tryGet

    let futs = newSeqWith(processCount, withRandDelay(ds.modify(key, incAsyncFn)))
    await allFutures(futs).wait(timeout)

    let finalValue = uint64.fromBytes((await ds.get(key)).tryGet)

    check finalValue.int == processCount

  test "should update value":
    (await ds.put(key, @((0.uint64).toBytes))).tryGet

    (await ds.modify(key, incAsyncFn)).tryGet

    let finalValue = uint64.fromBytes((await ds.get(key)).tryGet)

    check finalValue.int == 1

  test "should put value":
    (await ds.delete(key)).tryGet()

    proc returningSomeValue(_: ?seq[byte]): Future[?seq[byte]] {.async.} =
      return @(123.uint64.toBytes).some

    (await ds.modify(key, returningSomeValue)).tryGet

    let finalValue = uint64.fromBytes((await ds.get(key)).tryGet)

    check finalValue.int == 123

  test "should delete value":
    (await ds.put(key, @(0.uint64.toBytes))).tryGet

    proc returningNone(_: ?seq[byte]): Future[?seq[byte]] {.async.} =
      return seq[byte].none

    (await ds.modify(key, returningNone)).tryGet

    let hasKey = (await ds.has(key)).tryGet

    check not hasKey

  test "should return correct auxillary value":
    proc returningAux(_: ?seq[byte]): Future[(?seq[byte], seq[byte])] {.async.} =
      return (seq[byte].none, @[byte 123])

    let aux = (await ds.modifyGet(key, returningAux)).tryGet()

    check:
      aux == (123.byte).repeat(dsCount)

  test "should propagate exception as failure":
    proc throwing(a: ?seq[byte]): Future[?seq[byte]] {.async.} =
      raise newException(CatchableError, "some error msg")

    let res = await ds.modify(key, throwing)

    check:
      res.errorOption.map((err) => err.msg) == some("some error msg")
