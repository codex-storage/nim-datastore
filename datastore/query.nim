import ./key

type
  Node* = object of RootObj
    next*: Node
    prev*: Node

  Filter* = object of Node
    field*: string
    value*: string

  FilterBool* = object of Filter
    a*, b*: Filter

  FilterAnd = object of FilterBool
  FilterOr = object of FilterBool

  Eq = object of Filter
  Lt = object of Filter
  Gt = object of Filter
  Not = object of Filter

  SortOrder* {.pure.} = enum
    Assending,
    Descensing

  Order* = object
    field*: string
    sort*: SortOrder

  Query* = object
    key*: Key
    limit*: int
    skip*: int
    orders*: seq[Order]
    filters*: seq[Filter]

  QueryResponse* = tuple[key: Key, data: seq[byte]]

proc `==`*(a, b: Filter): Filter = discard

proc `!=`*(a, b: Filter): Filter = discard
proc `>`*(a, b: Filter): Filter = discard
proc `>=`*(a, b: Filter): Filter = discard
proc `<`*(a, b: Filter): Filter = discard
proc `<=`*(a, b: Filter): Filter = discard

proc init*(
  T: type Query,
  key: Key,
  orders: openArray[Order] = [],
  filters: openArray[Filter] = [],
  skip = 0,
  limit = 0): T =

  T(
    key: key,
    filters: @filters,
    orders: @orders,
    skip: skip,
    limit: limit)
