import std/options

import pkg/asynctest
import pkg/chronos
import pkg/stew/results

import pkg/datastore

proc basicStoreTests*(
  ds: Datastore,
  key: Key,
  bytes: seq[byte],
  otherBytes: seq[byte]) =

  test "put":
    (await ds.put(key, bytes)).tryGet()

  test "get":
    check:
      (await ds.get(key)).tryGet() == bytes

  test "put update":
    (await ds.put(key, otherBytes)).tryGet()

  test "get updated":
    check:
      (await ds.get(key)).tryGet() == otherBytes

  test "delete":
    (await ds.delete(key)).tryGet()

  test "contains":
    check:
      not (await ds.contains(key)).tryGet()
