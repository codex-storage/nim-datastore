import std/options
import std/sugar
import std/tables
import std/strutils

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/stew/endians2
import pkg/questionable
import pkg/questionable/results

import pkg/datastore/typedds
import pkg/datastore

proc encode(i: int): seq[byte] =
  @(cast[uint64](i).toBytesBE)

proc decode(T: type int, bytes: seq[byte]): ?!T =
  if bytes.len >= sizeof(uint64):
    success(cast[int](uint64.fromBytesBE(bytes)))
  else:
    failure("not enough bytes to decode int")

proc encode(s: string): seq[byte] =
  s.toBytes()

proc decode(T: type string, bytes: seq[byte]): ?!T =
  success(string.fromBytes(bytes))

proc typedDsTests*(
  ds: Datastore,
  key: Key,
  dsCount = 1) =

  let tds = TypedDatastore.init(ds)

  test "should put a value":
    (await tds.put(key, 11)).tryGet()

    check:
      (await tds.has(key)).tryGet()

  test "should get the value":
    (await tds.put(key, 22)).tryGet()

    check:
      22  == (await get[int](tds, key)).tryGet()

  test "should insert a value":
    proc returningSome(_: ?int): Future[?int] {.async.} =
      some(33)

    (await tds.delete(key)).tryGet()
    (await tds.modify(key, returningSome)).tryGet()

    check:
      (await tds.has(key)).tryGet()

  test "should delete a value":
    proc returningNone(_: ?int): Future[?int] {.async.} =
      int.none

    (await tds.put(key, 33)).tryGet()
    (await tds.modify(key, returningNone)).tryGet()

    check:
      not (await tds.has(key)).tryGet()

  test "should update a value":
    proc incrementing(maybeI: ?int): Future[?int] {.async.} =
      if i =? maybeI:
        some(i + 1)
      else:
        int.none

    (await tds.put(key, 33)).tryGet()
    (await tds.modify(key, incrementing)).tryGet()

    check:
      34 == (await get[int](tds, key)).tryGet()

  test "should update a value and get aux":
    proc returningAux(maybeI: ?int): Future[(?int, string)] {.async.} =
      if i =? maybeI:
        (some(i + 1), "foo")
      else:
        (int.none, "bar")

    (await tds.put(key, 44)).tryGet()

    check:
      "foo".repeat(dsCount) == (await tds.modifyGet(key, returningAux)).tryGet()
      45 == (await get[int](tds, key)).tryGet()

  test "should propagate exception as failure":
    proc throwing(a: ?int): Future[?int] {.async.} =
      raise newException(CatchableError, "some error msg")

    check:
      some("some error msg") == (await tds.modify(key, throwing)).errorOption.map((err) => err.msg)

proc typedDsQueryTests*(ds: Datastore) =

  let tds = TypedDatastore.init(ds)

  test "should query values":
    let
      source = {
        "a": 11,
        "b": 22,
        "c": 33,
        "d": 44
      }.toTable
      Root = Key.init("/querytest").tryGet()

    for k, v in source:
      let key = (Root / k).tryGet()
      (await tds.put(key, v)).tryGet()

    let iter = (await query[int](tds, Query.init(Root))).tryGet()

    var results = initTable[string, int]()

    while not iter.finished:
      let
        item = (await iter.next()).tryGet()

      without key =? item.key:
        continue

      let value = item.value.tryGet()

      check:
        key.value notin results

      results[key.value] = value

    check:
      results == source
