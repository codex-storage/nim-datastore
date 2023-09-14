import threading/smartptrs
import std/hashes

export hashes

type
  DataBufferHolder* = object
    buf: ptr UncheckedArray[byte]
    size: int

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

proc isNil*(a: DataBuffer): bool = smartptrs.isNil(a)

proc hash*(a: DataBuffer): Hash =
  a[].buf.toOpenArray(0, a[].size-1).hash()

proc `==`*(a, b: DataBuffer): bool =
  if a.isNil and b.isNil: return true
  elif a.isNil or b.isNil: return false
  elif a[].size != b[].size: return false
  elif a[].buf == b[].buf: return true
  else: a.hash() == b.hash()

proc new*(tp: type DataBuffer, size: int = 0): DataBuffer =
  ## allocate new buffer with given size
  ##

  newSharedPtr(DataBufferHolder(
    buf: cast[typeof(result[].buf)](allocShared0(size)),
    size: size,
  ))

proc new*[T: byte | char](tp: type DataBuffer, data: openArray[T]): DataBuffer =
  ## allocate new buffer and copies indata from openArray
  ##
  result = DataBuffer.new(data.len)
  if data.len() > 0:
    copyMem(result[].buf, unsafeAddr data[0], data.len)

converter toSeq*(self: DataBuffer): seq[byte] =
  ## convert buffer to a seq type using copy and either a byte or char
  ##

  result = newSeq[byte](self.len)
  if self.len() > 0:
    copyMem(addr result[0], unsafeAddr self[].buf[0], self.len)

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
    copyMem(addr result[0], unsafeAddr data[].buf[0], data.len)

proc `$`*(data: DataBuffer): string =
  ## convert buffer to string type using copy
  ##

  data.toString()

converter toBuffer*(err: ref CatchableError): DataBuffer =
  ## convert exception to an object with StringBuffer
  ##

  return DataBuffer.new(err.msg)
