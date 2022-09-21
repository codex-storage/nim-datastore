import pkg/upraises
import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./key
import ./types

type
  SortOrder* {.pure.} = enum
    Assending,
    Descending

  Query* = object
    key*: Key
    value*: bool
    limit*: int
    offset*: int
    sort*: SortOrder

  QueryResponse* = tuple[key: ?Key, data: seq[byte]]
  QueryEndedError* = object of DatastoreError

  GetNext* = proc(): Future[?!QueryResponse] {.upraises: [], gcsafe, closure.}
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

proc new*(T: type QueryIter): T =
  QueryIter(dispose: defaultDispose)

proc init*(
  T: type Query,
  key: Key,
  value = false,
  sort = SortOrder.Assending,
  offset = 0,
  limit = -1): T =

  T(
    key: key,
    value: value,
    sort: sort,
    offset: offset,
    limit: limit)
