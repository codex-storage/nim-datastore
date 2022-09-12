import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/upraises

import ./datastore

export datastore

push: {.upraises: [].}

type
  NullDatastore* = ref object of Datastore

proc new*(T: type NullDatastore): T =
  T()

method close*(self: NullDatastore) {.async, locks: "unknown".} =
  discard

method contains*(
  self: NullDatastore,
  key: Key): Future[?!bool] {.async, locks: "unknown".} =

  return success false

method delete*(
  self: NullDatastore,
  key: Key): Future[?!void] {.async, locks: "unknown".} =

  return success()

method get*(
  self: NullDatastore,
  key: Key): Future[?!(?seq[byte])] {.async, locks: "unknown".} =

  return success seq[byte].none

method put*(
  self: NullDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async, locks: "unknown".} =

  return success()

iterator queryImpl(
  datastore: Datastore,
  query: Query): Future[QueryResponse] {.closure.} =

  discard

method query*(self: NullDatastore): QueryIterator {.locks: "unknown".} =
  queryImpl
