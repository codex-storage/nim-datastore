import std/options

import pkg/asynctest/unittest2
import pkg/chronos
import pkg/stew/results

import ../../datastore
import ./templates

suite "Datastore (base)":
  let
    key = Key.init("a").get
    ds = Datastore()

  asyncTest "put":
    expect Defect: discard ds.put(key, @[1.byte])

  asyncTest "delete":
    expect Defect: discard ds.delete(key)

  asyncTest "contains":
    expect Defect: discard ds.contains(key)

  asyncTest "get":
    expect Defect: discard ds.get(key)

  asyncTest "query":
    expect Defect: discard ds.query
