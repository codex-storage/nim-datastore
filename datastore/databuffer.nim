


# import std/atomics
import threading/smartptrs


type
  DataBufferHolder* = object
    buf: ptr UncheckedArray[byte]
    size: int
  
  DataBuffer* = SharedPtr[DataBufferHolder]
  KeyBuffer* = DataBuffer
  ValueBuffer* = DataBuffer
  StringBuffer* = DataBuffer
  CatchableErrorBuffer* = object
    msg: StringBuffer

proc `=destroy`*(x: var DataBufferHolder) =
  ## copy pointer implementation
  if x.buf != nil:
    when isMainModule or true:
      echo "buffer: FREE: ", repr x.buf.pointer
    deallocShared(x.buf)

proc len*(a: DataBuffer): int = a[].size

proc new*(tp: typedesc[DataBuffer], size: int = 0): DataBuffer =
  newSharedPtr(DataBufferHolder(
    buf: cast[typeof(result[].buf)](allocShared0(size)),
    size: size,
  ))

proc new*[T: byte | char](tp: typedesc[DataBuffer], data: openArray[T]): DataBuffer =
  ## allocate new buffer and copies indata from openArray
  ## 
  result = DataBuffer.new(data.len)
  if data.len() > 0:
    copyMem(result[].buf, unsafeAddr data[0], data.len)

proc toSeq*[T: byte | char](a: DataBuffer, tp: typedesc[T]): seq[T] =
  result = newSeq[T](a.len)
  copyMem(addr result[0], unsafeAddr a[].buf[0], a.len)

proc toString*(data: DataBuffer): string =
  result = newString(data.len())
  if data.len() > 0:
    copyMem(addr result[0], unsafeAddr data[].buf[0], data.len)

proc toCatchable*(data: CatchableErrorBuffer): ref CatchableError =
  result = (ref CatchableError)(msg: data.msg.toString())

proc toBuffer*(err: ref Exception): CatchableErrorBuffer =
  return CatchableErrorBuffer(
    msg: StringBuffer.new(err.msg)
  )
