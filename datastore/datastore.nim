import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/upraises

import ./key

export key

push: {.upraises: [].}

type
  Datastore* = ref object of RootObj

method contains*(
  self: Datastore,
  key: Key): Future[?!bool] {.async, base, locks: "unknown".} =

  raiseAssert("Not implemented!")

method delete*(
  self: Datastore,
  key: Key): Future[?!void] {.async, base, locks: "unknown".} =

  raiseAssert("Not implemented!")

method get*(
  self: Datastore,
  key: Key): Future[?!(?seq[byte])] {.async, base, locks: "unknown".} =

  raiseAssert("Not implemented!")

method put*(
  self: Datastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async, base, locks: "unknown".} =

  raiseAssert("Not implemented!")

# method query*(
#   self: Datastore,
#   query: ...): Future[?!(?...)] {.async, base, locks: "unknown".} =
#
#   raiseAssert("Not implemented!")
