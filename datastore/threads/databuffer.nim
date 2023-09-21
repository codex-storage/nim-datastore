import threading/smartptrs
import std/hashes
import pkg/stew/ptrops

export hashes

type
  DataBufferOpt* = enum
    dbNullTerminate

  DataBufferHolder* = object
    buf: ptr UncheckedArray[byte]
    size: int
    cap: int

  DataBuffer* = SharedPtr[DataBufferHolder] ##\
    ## A fixed length data buffer using a SharedPtr.
    ## It is thread safe even with `refc` since
    ## it doesn't use string or seq types internally.
    ##

proc `=destroy`*(x: var DataBufferHolder) =
  ## copy pointer implementation
  ##

  if x.buf != nil:
    # echo "buffer: FREE: ", repr x.buf.pointer
    deallocShared(x.buf)

proc len*(a: DataBuffer): int = a[].size
proc capacity*(a: DataBuffer): int = a[].cap

proc isNil*(a: DataBuffer): bool = smartptrs.isNil(a)

proc hash*(a: DataBuffer): Hash =
  a[].buf.toOpenArray(0, a[].size-1).hash()

proc `[]`*(db: DataBuffer, idx: int): var byte =
  if idx >= db.len():
    raise newException(IndexDefect, "index out of bounds")
  db[].buf[idx]

proc `==`*(a, b: DataBuffer): bool =
  if a.isNil and b.isNil: return true
  elif a.isNil or b.isNil: return false
  elif a[].size != b[].size: return false
  elif a[].buf == b[].buf: return true
  else: a.hash() == b.hash()

template `==`*[T: char | byte](a: DataBuffer, b: openArray[T]): bool =
  if a.isNil: false
  elif a[].size != b.len: false
  else: a.hash() == b.hash()

proc new*(tp: type DataBuffer, capacity: int = 0): DataBuffer =
  ## allocate new buffer with given capacity
  ##

  newSharedPtr(DataBufferHolder(
    buf: cast[typeof(result[].buf)](allocShared0(capacity)),
    size: 0,
    cap: capacity,
  ))

proc new*[T: byte | char](tp: type DataBuffer, data: openArray[T], opts: set[DataBufferOpt] = {}): DataBuffer =
  ## allocate new buffer and copies indata from openArray
  ##
  let dataCap =
    if dbNullTerminate in opts: data.len() + 1
    else: data.len()
  result = DataBuffer.new(dataCap)
  if data.len() > 0:
    copyMem(result[].buf, baseAddr data, data.len())
  result[].size = data.len()

proc new*(tp: type DataBuffer, data: pointer, first, last: int): DataBuffer =
  DataBuffer.new(toOpenArray(cast[ptr UncheckedArray[byte]](data), first, last))

proc baseAddr*(db: DataBuffer): pointer =
  db[].buf

proc clear*(db: DataBuffer) =
  zeroMem(db[].buf, db[].cap)
  db[].size = 0

proc setData*[T: byte | char](db: DataBuffer, data: openArray[T]) =
  ## allocate new buffer and copies indata from openArray
  ##
  if data.len() > db[].cap:
    raise newException(IndexDefect, "data too large for buffer")
  db.clear() # this is expensive, but we can optimize later
  copyMem(db[].buf, baseAddr data, data.len())
  db[].size = data.len()

converter toSeq*(self: DataBuffer): seq[byte] =
  ## convert buffer to a seq type using copy and either a byte or char
  ##

  result = newSeq[byte](self.len)
  if self.len() > 0:
    copyMem(addr result[0], addr self[].buf[0], self.len)

proc `@`*(self: DataBuffer): seq[byte] =
  ## Convert a buffer to a seq type using copy and
  ## either a byte or char
  ##

  self.toSeq()

converter toString*(data: DataBuffer): string =
  ## convert buffer to string type using copy
  ##

  if data.isNil: return ""
  result = newString(data.len())
  if data.len() > 0:
    copyMem(addr result[0], addr data[].buf[0], data.len)

proc `$`*(data: DataBuffer): string =
  ## convert buffer to string type using copy
  ##

  data.toString()

converter toBuffer*(err: ref CatchableError): DataBuffer =
  ## convert exception to an object with StringBuffer
  ##

  return DataBuffer.new(err.msg)

template toOpenArray*[T: byte | char](data: DataBuffer, t: typedesc[T]): openArray[T] =
  ## get openArray from DataBuffer as char
  ## 
  ## this is explicit since sqlite treats string differently from openArray[byte]
  let bf = cast[ptr UncheckedArray[T]](data[].buf)
  bf.toOpenArray(0, data[].size-1)
