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

method contains*(
  self: NullDatastore,
  key: Key): ?!bool {.locks: "unknown".} =

  success false

method delete*(
  self: NullDatastore,
  key: Key): ?!void {.locks: "unknown".} =

  success()

method get*(
  self: NullDatastore,
  key: Key): ?!(?seq[byte]) {.locks: "unknown".} =

  success seq[byte].none

method put*(
  self: NullDatastore,
  key: Key,
  data: openArray[byte]): ?!void {.locks: "unknown".} =

  success()

# method query*(
#   self: NullDatastore,
#   query: ...): ?!(?...) {.locks: "unknown".} =
#
#   success ....none
