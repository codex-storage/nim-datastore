import std/options

import pkg/stew/results
import pkg/unittest2

import ../../datastore/null_datastore

suite "NullDatastore":
  setup:
    let
      key = Key.init("a").get
      ds = NullDatastore.new()

    discard key # suppresses "declared but not used" re: key

  test "new":
    check: not ds.isNil

  test "put":
    check: ds.put(key, [1.byte]).isOk

  test "delete":
    check: ds.delete(key).isOk

  test "contains":
    check:
      ds.contains(key).isOk
      ds.contains(key).get == false

  test "get":
    check:
      ds.get(key).isOk
      ds.get(key).get.isNone

  # test "query":
  #   check:
  #     ds.query(...).isOk
  #     ds.query(...).get.isNone
