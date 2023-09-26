import pkg/questionable/results
import pkg/upraises

import std/algorithm
import std/options
import ./threads/databuffer
import ./threads/threadresult
import ./threads/semaphore
import ./key
import ./types

export databuffer, threadresult, semaphore, types
export upraises, results, SortOrder

type
  KeyId* = object
    ## serialized Key ID, equivalent to `key.id()`
    data*: DataBuffer

  DbKey* = string | KeyId
  DbVal* = seq[byte] | DataBuffer

  DbBatchEntry*[K, V] = tuple[key: K, data: V]

  DbQuery*[K] = object
    key*: K         # Key to be queried
    value*: bool      # Flag to indicate if data should be returned
    limit*: int       # Max items to return - not available in all backends
    offset*: int      # Offset from which to start querying - not available in all backends
    sort*: SortOrder  # Sort order - not available in all backends

  DbQueryHandle*[K, T] = object
    query*: DbQuery[K]
    cancel*: bool
    closed*: bool
    env*: T

  DbQueryResponse* = tuple[key: Option[KeyId], data: DataBuffer]

proc `$`*(id: KeyId): string = $(id.data)

proc toKey*(tp: typedesc[KeyId], id: cstring): KeyId = KeyId.new(id)
proc toKey*(tp: typedesc[string], id: cstring): string = $(id)

template toVal*(tp: typedesc[DataBuffer], id: openArray[byte]): DataBuffer = DataBuffer.new(id)
template toVal*(tp: typedesc[seq[byte]], id: openArray[byte]): seq[byte] = @(id)

proc new*(tp: typedesc[KeyId], id: cstring): KeyId =
  ## copy cstring including null terminator
  KeyId(data: DataBuffer.new(id.toOpenArray(0, id.len()-1)))

proc new*(tp: typedesc[KeyId], id: string): KeyId =
  ## copy cstring including null terminator
  KeyId(data: DataBuffer.new(id))

# proc toCString*(key: KeyId): cstring =
#   ## copy cstring including null terminator
#   cast[cstring](baseAddr key.data)

# proc toDb*(key: Key): DbKey {.inline, raises: [].} =
#   let id: string = key.id()
#   let db = DataBuffer.new(id.len()+1) # include room for null for cstring compat
#   db.setData(id)
#   DbKey(data: db)

proc toKey*(key: KeyId): Key {.inline, raises: [].} =
  Key.init(key.data).expect("expected valid key here for but got `" & $key.data & "`")

template toOpenArray*(x: DbKey): openArray[char] =
  x.data.toOpenArray(char)
