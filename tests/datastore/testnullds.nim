import std/options

import pkg/asynctest/unittest2
import pkg/chronos
import pkg/stew/results

import pkg/datastore/nullds

suite "NullDatastore":
  let
    key = Key.init("a").get
    ds = NullDatastore.new()

  test "new":
    check: not ds.isNil

  test "put":
    check: (await ds.put(key, @[1.byte])).isOk

  test "delete":
    check: (await ds.delete(key)).isOk

  test "contains":
    check:
      (await ds.contains(key)).isOk
      (await ds.contains(key)).get == false

  test "get":
    check:
      (await ds.get(key)).isOk
      (await ds.get(key)).get.isNone

  test "query":
    var
      x = true

    for n in ds.query(Query.init(key)):
      # `iterator query` for NullDatastore never yields so the following lines
      # are not run (else the test would hang)
      x = false
      discard (await n)

    check: x
