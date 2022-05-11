import std/options

import pkg/stew/results
import pkg/unittest2

import ../../datastore

const
  oneByte = @[1.byte]

suite "Datastore (base)":
  setup:
    let
      key = Key.init("a").get
      ds = Datastore()

  test "put":
    expect Defect: discard ds.put(key, oneByte)

  test "delete":
    expect Defect: discard ds.delete(key)

  test "contains":
    expect Defect: discard ds.contains(key)

  test "get":
    expect Defect: discard ds.get(key)

  # test "query":
  #   expect Defect: discard ds.query(...)
