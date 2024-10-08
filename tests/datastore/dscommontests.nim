import pkg/asynctest/chronos/unittest2
import pkg/chronos

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
      not await (key in ds)

  test "put batch":
    var
      batch: seq[BatchEntry]

    for k in 0..<100:
      batch.add((Key.init(key.id, $k).tryGet, @[k.byte]))

    (await ds.put(batch)).tryGet

    for k in batch:
      check: (await ds.has(k.key)).tryGet

  test "delete batch":
    var
      batch: seq[Key]

    for k in 0..<100:
      batch.add(Key.init(key.id, $k).tryGet)

    (await ds.delete(batch)).tryGet

    for k in batch:
      check: not (await ds.has(k)).tryGet
