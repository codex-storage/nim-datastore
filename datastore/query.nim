
{.push raises: [].}

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
    key*: Key         # Key to be queried
    value*: bool      # Flag to indicate if data should be returned
    limit*: int       # Max items to return - not available in all backends
    offset*: int      # Offset from which to start querying - not available in all backends
    sort*: SortOrder  # Sort order - not available in all backends

  QueryResponse* = tuple[key: ?Key, data: seq[byte]]
  QueryEndedError* = object of DatastoreError

  GetNext* = proc(): Future[?!QueryResponse] {.raises: [], gcsafe, closure.}
  IterDispose* = proc(): Future[?!void] {.raises: [], gcsafe.}
  QueryIter* = ref object
    finished*: bool
    next*: GetNext
    dispose*: IterDispose

iterator items*(q: QueryIter): Future[?!QueryResponse] =
  while not q.finished:
    yield q.next()

proc defaultDispose(): Future[?!void] {.gcsafe, async.} =
  return success()

proc new*(T: type QueryIter, dispose = defaultDispose): T =
  QueryIter(dispose: dispose)

proc init*(
  T: type Query,
  key: Key,
  value = true,
  sort = SortOrder.Assending,
  offset = 0,
  limit = -1): T =

  T(
    key: key,
    value: value,
    sort: sort,
    offset: offset,
    limit: limit)
