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
  QueryIter* = iterator(): QueryResponse {.closure.}

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
