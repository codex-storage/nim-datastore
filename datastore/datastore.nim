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
  key: Key): ?!bool {.base, locks: "unknown".} =

  raiseAssert("Not implemented!")

method delete*(
  self: Datastore,
  key: Key): ?!void {.base, locks: "unknown".} =

  raiseAssert("Not implemented!")

method get*(
  self: Datastore,
  key: Key): ?!(?seq[byte]) {.base, locks: "unknown".} =

  raiseAssert("Not implemented!")

method put*(
  self: Datastore,
  key: Key,
  data: openArray[byte]): ?!void {.base, locks: "unknown".} =

  raiseAssert("Not implemented!")

# method query*(
#   self: Datastore,
#   query: ...): ?!(?...) {.base, locks: "unknown".} =
#
#   raiseAssert("Not implemented!")
