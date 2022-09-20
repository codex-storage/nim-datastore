import ./key

type
  Query* = object
    key: QueryKey

  QueryKey* = Key

  QueryResponse* = tuple[key: Key, data: seq[byte]]

proc init*(
  T: type Query,
  key: QueryKey): T =

  T(key: key)

proc key*(self: Query): QueryKey =
  self.key
