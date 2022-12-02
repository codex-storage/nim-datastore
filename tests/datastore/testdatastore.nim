import std/options

import pkg/asynctest/unittest2
import pkg/chronos
import pkg/stew/results

import pkg/datastore

suite "Datastore (base)":
  let
    key = Key.init("a").get
    ds = Datastore()

  test "put":
    expect Defect: discard ds.put(key, @[1.byte])

  test "delete":
    expect Defect: discard ds.delete(key)

  test "contains":
    expect Defect: discard ds.has(key)

  test "get":
    expect Defect: discard ds.get(key)

  test "query":
    expect Defect:
      let iter = (await ds.query(Query.init(key))).tryGet
      for n in iter: discard
