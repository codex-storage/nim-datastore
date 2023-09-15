import std/hashes

import ./sharedptr

export hashes
export sharedptr

type
  DataBufferHolder* = object
    buf: ptr UncheckedArray[byte]
    size: int
  
  DataBuffer* = SharedPtr[DataBufferHolder] ##\
    ## A fixed length data buffer using a SharedPtr.
    ## It is thread safe even with `refc` since
    ## it doesn't use string or seq types internally.
    ## 

  KeyBuffer* = DataBuffer
  ValueBuffer* = DataBuffer
  # StringBuffer* = DataBuffer
  CatchableErrorBuffer* = object
    msg: DataBuffer


proc `=destroy`*(x: var DataBufferHolder) =
  ## copy pointer implementation
  if x.buf != nil:
    # when isMainModule or true:
    echoed "databuffer: dealloc: ", repr x.buf.pointer
    deallocShared(x.buf)

proc len*(a: DataBuffer): int = a[].size

proc hash*(a: DataBuffer): Hash =
  a[].buf.toOpenArray(0, a[].size-1).hash()

proc `==`*(a, b: DataBuffer): bool =
  if a.isNil and b.isNil: return true
  elif a.isNil or b.isNil: return false
  elif a[].size != b[].size: return false
  elif a[].buf == b[].buf: return true
  else: a.hash() == b.hash()

proc new*(tp: typedesc[DataBuffer], size: int = 0): DataBuffer =
  ## allocate new buffer with given size
  result = newSharedPtr(DataBufferHolder(
    buf: cast[typeof(result[].buf)](allocShared0(size)),
    size: size,
  ))
  # echoed "DataBuffer:new: ", result.unsafeRawPtr().repr,
  #       " tp ", $(typeof(D)),
  #       " @ ", result[].buf.pointer.repr

proc new*[T: byte | char; D: DataBuffer](tp: typedesc[D], data: openArray[T]): D =
  ## allocate new buffer and copies indata from openArray
  ## 
  result = D.new(data.len)
  if data.len() > 0:
    copyMem(result[].buf, unsafeAddr data[0], data.len)

proc toSeq*[T: byte | char](a: DataBuffer, tp: typedesc[T]): seq[T] =
  ## convert buffer to a seq type using copy and either a byte or char
  result = newSeq[T](a.len)
  copyMem(addr result[0], unsafeAddr a[].buf[0], a.len)

proc toString*(data: DataBuffer): string =
  ## convert buffer to string type using copy
  if data.isNil: return "nil"
  result = newString(data.len())
  if data.len() > 0:
    copyMem(addr result[0], unsafeAddr data[].buf[0], data.len)

proc toCatchable*(err: CatchableErrorBuffer): ref CatchableError =
  ## convert back to a ref CatchableError
  result = (ref CatchableError)(msg: err.msg.toString())

proc toBuffer*(err: ref Exception): CatchableErrorBuffer =
  ## convert exception to an object with StringBuffer
  echoed "DataBuffer:toBuffer:err: ", err.msg
  return CatchableErrorBuffer(
    msg: DataBuffer.new(err.msg)
  )

import ../key
import stew/results

proc new*(tp: typedesc[KeyBuffer], key: Key): KeyBuffer =
  let ks = key.id()
  result = KeyBuffer.new(ks)
  # echoed "KeyBuffer:new: ", $result
proc toKey*(kb: KeyBuffer): Key =
  let ks = kb.toString()
  # echo "toKey: ", ks
  let res = Key.init(ks)
  res.expect("should always be valid")
