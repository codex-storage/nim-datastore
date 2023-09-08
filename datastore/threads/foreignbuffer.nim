
type
  ## Copy foreign buffers between threads.
  ##
  ## This is meant to be used as a temporary holder a
  ## pointer to a foreign buffer that is being passed
  ## between threads.
  ##
  ## The receiving thread should copy the contained buffer
  ## to it's local GC as soon as possible. Should only be
  ## used with refgc.
  ##
  ForeignBuff*[T] = object
    buf: ptr UncheckedArray[T]
    len: int
    cell: ForeignCell

proc `=sink`[T](a: var ForeignBuff[T], b: ForeignBuff[T]) =
  `=destroy`(a)
  wasMoved(a)
  a.len = b.len
  a.buf = b.buf
  a.cell = b.cell

proc `=copy`[T](a: var ForeignBuff[T], b: ForeignBuff[T])
  {.error: "You can't copy the buffer, only it's contents!".}

proc `=destroy`[T](self: var ForeignBuff[T]) =
  if self.cell.data != nil:
    echo "DESTROYING CELL"
    dispose self.cell

proc len*[T](self: ForeignBuff[T]): int =
  return self.len

template `[]`*[T](self: ForeignBuff[T], idx: int): T =
  assert idx >= 0 and idx < self.len
  return self.buf[idx]

template `[]=`*[T](self: ForeignBuff[T], idx: int, val: T) =
  assert idx >= 0 and idx < self.len
  return self.buf[idx]

proc get*[T](self: ForeignBuff[T]): ptr UncheckedArray[T] =
  self.buf

iterator items*[T](self: ForeignBuff[T]): T =
  for i in 0 ..< self.len:
    yield self.buf[i]

iterator miterms*[T](self: ForeignBuff[T]): var T =
  for i in 0 ..< self.len:
    yield self.buf[i]

proc attach*[T](
  self: var ForeignBuff[T],
  buf: ptr UncheckedArray[T],
  len: int,
  cell: ForeignCell) =
  ## Attach a foreign pointer to this buffer
  ##

  self.buf = buf
  self.len = len
  self.cell = cell

func init*[T](_: type ForeignBuff[T]): ForeignBuff[T] =
  return ForeignBuff[T]()
