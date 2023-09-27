import pkg/upraises
import std/algorithm
import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./key
import ./types
import ./backend

export types
export options, SortOrder

type

  ## Front end types
  Query* = DbQuery[Key]

  QueryResponse* = DbQueryResponse[Key, seq[byte]]

  GetNext* = proc(): Future[?!QueryResponse] {.upraises: [], gcsafe.}
  IterDispose* = proc(): Future[?!void] {.upraises: [], gcsafe.}
  QueryIter* = ref object
    finished*: bool
    next*: GetNext
    dispose*: IterDispose

iterator items*(q: QueryIter): Future[?!QueryResponse] =
  while not q.finished:
    yield q.next()

proc defaultDispose(): Future[?!void] {.upraises: [], gcsafe, async.} =
  return success()

proc new*(T: type QueryIter, dispose = defaultDispose): T =
  QueryIter(dispose: dispose)

proc init*(T: type Query,
           key: Key,
           value = false,
           sort = SortOrder.Ascending,
           offset = 0,
           limit = -1): Query =
  dbQuery[Key](key, value, sort, offset, limit)

proc toKey*(key: KeyId): Key {.inline, raises: [].} =
  Key.init($key.data).expect("expected valid key here for but got `" & $key.data & "`")
