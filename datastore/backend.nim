import pkg/questionable/results
import pkg/upraises

import ./threads/databuffer
import ./threads/threadresult
import ./threads/semaphore
import ./key
import ./types

export databuffer, threadresult, semaphore, types
export upraises, results

type
  KeyId* = object
    ## serialized Key ID, equivalent to `key.id()`
    data: DataBuffer

  DbKey* = string | KeyId
  DbVal* = seq[byte] | DataBuffer

proc `$`*(id: KeyId): string = $(id.data)

proc toDb*(key: Key): DbKey {.inline, raises: [].} =
  let id: string = key.id()
  let db = DataBuffer.new(id.len()+1) # include room for null for cstring compat
  db.setData(id)
  DbKey(data: db)

proc toKey*(key: DbKey): Key {.inline, raises: [].} =
  Key.init(key.data).expect("expected valid key here for but got `" & $key.data & "`")

template toOpenArray*(x: DbKey): openArray[char] =
  x.data.toOpenArray(char)
