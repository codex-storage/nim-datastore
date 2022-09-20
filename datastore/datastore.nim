import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/upraises

import ./key
import ./query
import ./types

export key, query, types

push: {.upraises: [].}

method contains*(self: Datastore, key: Key): Future[?!bool] {.base, locks: "unknown".} =
  raiseAssert("Not implemented!")

method delete*(self: Datastore, key: Key): Future[?!void] {.base, locks: "unknown".} =
  raiseAssert("Not implemented!")

method get*(self: Datastore, key: Key): Future[?!seq[byte]] {.base, locks: "unknown".} =
  raiseAssert("Not implemented!")

method put*(self: Datastore, key: Key, data: seq[byte]): Future[?!void] {.base, locks: "unknown".} =
  raiseAssert("Not implemented!")

method close*(self: Datastore): Future[?!void] {.base, async, locks: "unknown".} =
  raiseAssert("Not implemented!")

method query*(
  self: Datastore,
  query: Query): Future[?!QueryIter] {.gcsafe.} =

  raiseAssert("Not implemented!")
