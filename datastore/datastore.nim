import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/upraises

import ./key
import ./query

export key, query

push: {.upraises: [].}

type
  Datastore* = ref object of RootObj

  QueryIterator* = iterator (
    datastore: Datastore,
    query: Query): Future[QueryResponse] {.closure, gcsafe.}

method close*(self: Datastore): Future[void] {.base, locks: "unknown".} =
  raiseAssert("Not implemented!")

method contains*(
  self: Datastore,
  key: Key): Future[?!bool] {.base, locks: "unknown".} =

  raiseAssert("Not implemented!")

method delete*(
  self: Datastore,
  key: Key): Future[?!void] {.base, locks: "unknown".} =

  raiseAssert("Not implemented!")

method get*(
  self: Datastore,
  key: Key): Future[?!(?seq[byte])] {.base, locks: "unknown".} =

  raiseAssert("Not implemented!")

method put*(
  self: Datastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.base, locks: "unknown".} =

  raiseAssert("Not implemented!")

method query*(self: Datastore): QueryIterator {.base, locks: "unknown".} =
  raiseAssert("Not implemented!")
