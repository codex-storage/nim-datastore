#            Nim's Runtime Library
#        (c) Copyright 2021 Nim contributors

import std/atomics
import std/isolation
import std/typetraits
import std/strutils

export isolation

proc raiseNilAccess() {.noinline.} =
  raise newException(NilAccessDefect, "dereferencing nil smart pointer")

template checkNotNil(p: typed) =
  when compileOption("boundChecks"):
    {.line.}:
      if p.isNil:
        raiseNilAccess()


when not defined(datastoreEchoedThread):
  proc echoed*(vals: varargs[string]) =
    discard
else:
  import std/terminal
  proc echoed*(vals: varargs[string]) =
    proc printThread(): ForegroundColor = 
      let tclr = [fgRed, fgGreen, fgBlue, fgMagenta, fgCyan]
      let tid = getThreadId()
      let color = tclr[(tid mod (tclr.len() - 1))]
      stdout.styledWrite(styleBright, fgWhite, "(thr: ", color, $tid, fgWhite, ")  ")
      color
    try:
      let color = printThread()
      var i = 0
      if vals.len() mod 2 != 0:
        stdout.styledWrite(color, vals[i])
        i.inc()
      while i + 1 < vals.len():
        stdout.styledWrite(color, vals[i], fgDefault, vals[i+1])
        i.inc(2)
      stdout.styledWrite("\n")
    except:
      discard
    finally:
      discard

type
  SharedPtr*[T] = object
    ## Shared ownership reference counting pointer.
    container*: ptr tuple[value: T, cnt: int, manual: bool]

proc incr*[T](a: SharedPtr[T]) =
  if a.container != nil:
    let res = atomicAddFetch(a.cnt, 1, ATOMIC_RELAXED)
    echoed "SharedPtr: incr: ", res

proc decr*[T](x: SharedPtr[T]) =
  if x.container != nil:
    let res = atomicSubFetch(addr x.container.cnt, 1, ATOMIC_RELEASE)
    if res == 0:
      echoed "SharedPtr: FREE: ", $x.container.pointer.repr, " cnt: ", $x.container.cnt, " tp: ", $(typeof(T))
      when compiles(`=destroy`(x[])):
        echoed "SharedPtr:call:child:destructor: ", $(typeof(x[]))
        `=destroy`(x[])
      else:
        echoed "SharedPtr:NOT CALLED:child:destructor: ", $(typeof(x[]))
      deallocShared(x.container)
      # x.container = nil # alas
    else:
      echoed "SharedPtr: decr: ", $x.container.pointer.repr, " cnt: ", $x.container.cnt, " tp: ", $(typeof(T))

proc release*[T](x: var SharedPtr[T]) =
  echoed "SharedPtr: release: ", $(typeof(T))
  x.decr()
  x.container = nil

proc `=destroy`*[T](x: var SharedPtr[T]) =
  if x.container != nil and not x.container.manual:
    echoed "SharedPtr: destroy: ", $x.container.pointer.repr, " cnt: ", $x.container.cnt, " tp: ", $(typeof(T))
    decr(x)

proc `=dup`*[T](src: SharedPtr[T]): SharedPtr[T] =
  if src.container != nil and not src.container.manual:
    echoed "SharedPtr: dup: ", src.container.pointer.repr, " cnt: ", src.container.cnt, " tp: ", $(typeof(T))
    discard atomicAddFetch(src.cnt, 1, ATOMIC_RELAXED)
  result.container = src.container
  result.cnt = src.cnt

proc `=copy`*[T](dest: var SharedPtr[T], src: SharedPtr[T]) =
  if src.container != nil and not src.container.manual:
    # echo "SharedPtr: copy: ", src.container.pointer.repr
    discard atomicAddFetch(addr src.container.cnt, 1, ATOMIC_RELAXED)
    echoed "SharedPtr: copy:src: ", $src.container.pointer.repr, " cnt: ", $src.container.cnt, " tp: ", $(typeof(T))
  if dest.container != nil:
    echoed "SharedPtr: copy:dest: ", $dest.container.pointer.repr, " cnt: ", $dest.container.cnt, " tp: ", $(typeof(T))
  `=destroy`(dest)
  dest.container = src.container

proc newSharedPtr*[T](val: sink Isolated[T], manualCount: Natural = 0): SharedPtr[T] {.nodestroy.} =
  ## Returns a shared pointer which shares,
  ## ownership of the object by reference counting.
  result.container = cast[typeof(result.container)](allocShared(sizeof(result.container[])))
  result.container.cnt = max(1, manualCount)
  result.container.manual = manualCount > 0
  result.container.value = extract val
  echoed "SharedPtr: alloc: ", $result.container.pointer.repr, " cnt: ", $result.container.cnt, " tp: ", $(typeof(T))

template newSharedPtr*[T](val: T): SharedPtr[T] =
  newSharedPtr(isolate(val))

proc newSharedPtr*[T](t: typedesc[T], manualCount: Natural = 0): SharedPtr[T] =
  ## Returns a shared pointer. It is not initialized,
  ## ownership of the object by reference counting.
  result.container = cast[typeof(result.container)](allocShared0(sizeof(result.container[])))
  result.container.cnt = max(1, manualCount)
  result.container.manual = manualCount > 0
  echoed "SharedPtr: alloc: ", $result.container.pointer.repr, " cnt: ", $result.container.cnt, " tp: ", $(typeof(T))

proc isNil*[T](p: SharedPtr[T]): bool {.inline.} =
  p.container == nil

proc `[]`*[T](p: SharedPtr[T]): var T {.inline.} =
  checkNotNil(p)
  p.container.value

proc `[]=`*[T](p: SharedPtr[T], val: sink Isolated[T]) {.inline.} =
  checkNotNil(p)
  p.container[] = extract val

template `[]=`*[T](p: SharedPtr[T]; val: T) =
  `[]=`(p, isolate(val))

proc `$`*[T](p: SharedPtr[T]): string {.inline.} =
  if p.container == nil: "nil\"\""
  else: p.container.pointer.repr & "\"" & $p.container[] & "\""
