


import std/os
import std/locks
import std/atomics

import events


type
  DataBuffer* = object
    cnt: ptr int
    buf: ptr UncheckedArray[byte]
    size: int
  
  KeyBuffer* = DataBuffer
  ValueBuffer* = DataBuffer

proc `$`*(data: DataBuffer): string =
  if data.buf.isNil:
    result = "nil"
  else:
    let sz = min(16, data.size)
    result = newString(sz + 2)
    copyMem(addr result[1], data.buf, sz)
    result[0] = '<'
    result[^1] = '>'

proc `=destroy`*(x: var DataBuffer) =
  ## copy pointer implementation
  if x.buf != nil and x.cnt != nil:
    let res = atomicSubFetch(x.cnt, 1, ATOMIC_ACQUIRE)
    if res == 0:
      when isMainModule:
        echo "buffer: FREE: ", repr x.buf.pointer, " ", x.cnt[]
      deallocShared(x.buf)
      deallocShared(x.cnt)
    else:
      when isMainModule:
        echo "buffer: decr: ", repr x.buf.pointer, " ", x.cnt[]

proc `=copy`*(a: var DataBuffer; b: DataBuffer) =
  ## copy pointer implementation

  # do nothing for self-assignments:
  if a.buf == b.buf: return
  `=destroy`(a)
  discard atomicAddFetch(b.cnt, 1, ATOMIC_RELAXED)
  a.size = b.size
  a.buf = b.buf
  a.cnt = b.cnt
  when isMainModule:
    echo "buffer: Copy: repr: ", b.cnt[],
          " ", repr a.buf.pointer, 
          " ", repr b.buf.pointer

proc incrAtomicCount*(a: DataBuffer) =
  let res = atomicAddFetch(a.cnt, 1, ATOMIC_RELAXED)
proc unsafeGetAtomicCount*(a: DataBuffer): int =
  atomicLoad(a.cnt, addr result, ATOMIC_RELAXED)

proc len*(a: DataBuffer): int = a.size

proc toSeq*[T: byte | char](a: DataBuffer, tp: typedesc[T]): seq[T] =
  result = newSeq[T](a.len)
  copyMem(addr result[0], unsafeAddr a.buf[0], a.len)

proc new*(tp: typedesc[DataBuffer], size: int = 0): DataBuffer =
  let cnt = cast[ptr int](allocShared0(sizeof(result.cnt)))
  cnt[] = 1
  DataBuffer(
    cnt: cnt,
    buf: cast[typeof(result.buf)](allocShared0(size)),
    size: size,
  )

proc new*[T: byte | char](tp: typedesc[DataBuffer], data: openArray[T]): DataBuffer =
  ## allocate new buffer and copies indata from openArray
  ## 
  result = DataBuffer.new(data.len)
  if data.len() > 0:
    copyMem(result.buf, unsafeAddr data[0], data.len)
