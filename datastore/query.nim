import pkg/upraises
import pkg/chronos

import ./key

type
  SortOrder* {.pure.} = enum
    Assending,
    Descensing

  Query* = object
    key*: Key
    value*: bool
    limit*: int
    skip*: int
    sort*: SortOrder

  QueryResponse* = tuple[key: Key, data: seq[byte]]

  GetNext* = proc(): Future[QueryResponse] {.upraises: [], gcsafe, closure.}
  QueryIter* = object
    finished: bool
    next*: GetNext

iterator items*(q: QueryIter): Future[QueryResponse] =
  while not q.finished:
    yield q.next()

proc init*(
  T: type Query,
  key: Key,
  value = false,
  sort = SortOrder.Descensing,
  skip = 0,
  limit = 0): T =

  T(
    key: key,
    value: value,
    sort: sort,
    skip: skip,
    limit: limit)
