import std/options

import pkg/asynctest/unittest2
import pkg/chronos
import pkg/stew/results

import ../../datastore/null_datastore
import ./templates

suite "NullDatastore":
  let
    key = Key.init("a").get
    ds = NullDatastore.new()

  asyncTest "new":
    check: not ds.isNil

  asyncTest "put":
    check: (await ds.put(key, @[1.byte])).isOk

  asyncTest "delete":
    check: (await ds.delete(key)).isOk

  asyncTest "contains":
    check:
      (await ds.contains(key)).isOk
      (await ds.contains(key)).get == false

  asyncTest "get":
    check:
      (await ds.get(key)).isOk
      (await ds.get(key)).get.isNone

  asyncTest "query":
    var
      x = true

    let q = ds.query; for n in q(ds, Query.init(key)):
      # `iterator query` for NullDatastore never yields so the following lines
      # are not run (else the test would hang)
      x = false
      discard (await n)

    check: x
