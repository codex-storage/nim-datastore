import pkg/chronos
import pkg/questionable/results
import pkg/upraises

import ./key
import ./query
import ./types

export key, query, types

push: {.upraises: [].}

type
  BatchEntry* = tuple[key: Key, data: seq[byte]]

method has*(self: Datastore, key: Key): Future[?!bool] {.base, locks: "unknown", raises: [].} =
  raiseAssert("Not implemented!")

method delete*(self: Datastore, key: Key): Future[?!void] {.base, locks: "unknown", raises: [].} =
  raiseAssert("Not implemented!")

method delete*(self: Datastore, keys: seq[Key]): Future[?!void] {.base, locks: "unknown", raises: [].} =
  raiseAssert("Not implemented!")

method get*(self: Datastore, key: Key): Future[?!seq[byte]] {.base, locks: "unknown", raises: [].} =
  raiseAssert("Not implemented!")

method put*(self: Datastore, key: Key, data: seq[byte]): Future[?!void] {.base, locks: "unknown", raises: [].} =
  raiseAssert("Not implemented!")

method put*(self: Datastore, batch: seq[BatchEntry]): Future[?!void] {.base, locks: "unknown", raises: [].} =
  raiseAssert("Not implemented!")

method close*(self: Datastore): Future[?!void] {.base, locks: "unknown", raises: [].} =
  raiseAssert("Not implemented!")

method query*(
  self: Datastore,
  query: Query): Future[?!QueryIter] {.base, gcsafe, raises: [].} =

  raiseAssert("Not implemented!")

proc contains*(self: Datastore, key: Key): Future[bool] {.async, raises: [].} =
  return (await self.has(key)) |? false
