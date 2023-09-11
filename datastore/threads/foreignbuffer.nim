
import std/locks

type
  ## Pass foreign buffers between threads.
  ##
  ## This is meant to be used as temporary holder
  ## pointer to a foreign buffer that is being passed
  ## between threads.
  ##
  ## The receiving thread should copy the contained buffer
  ## to it's local GC as soon as possible. Should only be
  ## used with refgc.
  ##
  ForeignBuff*[T] = object
    lock: Lock
    buf: ptr UncheckedArray[T]
    len: int
    cell: ForeignCell

proc `=sink`[T](self: var ForeignBuff[T], b: ForeignBuff[T]) =
  withLock(self.lock):
    `=destroy`(self)
    wasMoved(self)
    self.len = b.len
    self.buf = b.buf
    self.cell = b.cell

proc `=copy`[T](self: var ForeignBuff[T], b: ForeignBuff[T]) {.error.}

proc `=destroy`[T](self: var ForeignBuff[T]) =
  withLock(self.lock):
    if self.cell.data != nil:
      echo "DESTROYING CELL"
      dispose self.cell

proc len*[T](self: ForeignBuff[T]): int =
  return self.len

proc get*[T](self: ForeignBuff[T]): ptr UncheckedArray[T] =
  self.buf

proc attach*[T](
  self: var ForeignBuff[T],
  buf: openArray[T]) =
  ## Attach self foreign pointer to this buffer
  ##
  withLock(self.lock):
    self.buf = makeUncheckedArray[T](baseAddr buf)
    self.len = buf.len
    self.cell = protect(self.buf)

func attached*[T]() =
  ## Check if self foreign pointer is attached to this buffer
  ##
  withLock(self.lock):
    return self.but != nil and self.cell.data != nil

## NOTE: Converters might return copies of the buffer,
## this should be overall safe since we want to copy
## the buffer local GC anyway.
converter toSeq*[T](self: ForeignBuff[T]): seq[T] | lent seq[T] =
  @(self.buf.toOpenArray(0, self.len - 1))

converter toString*[T](self: ForeignBuff[T]): string | lent string =
  $(self.buf.toOpenArray(0, self.len - 1))

converter getVal*[T](self: ForeignBuff[T]): ptr UncheckedArray[T] =
  self.buf

func init*[T](_: type ForeignBuff[T]): ForeignBuff[T] =
  var
    lock = Lock()

  lock.initLock()
  ForeignBuff[T](lock: lock)
