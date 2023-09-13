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

type
  SharedPtr*[T] = object
    ## Shared ownership reference counting pointer.
    container*: ptr tuple[value: T, cnt: int]

proc incr*[T](a: var SharedPtr[T]) =
  if a.container != nil and a.cnt != nil:
    let res = atomicAddFetch(a.cnt, 1, ATOMIC_RELAXED)
    echo "SharedPtr: manual incr: ", res

proc decr*[T](x: var SharedPtr[T]) =
  if x.container != nil:
    let res = atomicSubFetch(addr x.container.cnt, 1, ATOMIC_ACQUIRE)
    if res == 0:
      echo "SharedPtr: FREE: ", x.container[].repr, " tp: ", $(typeof(T))
      when compiles(`=destroy`(x[])):
        echo "DECR FREE: ", $(typeof(x[]))
        `=destroy`(x[])
      deallocShared(x.container)
      x.container = nil
    else:
      echo "SharedPtr: decr: ", x.container[].repr, " tp: ", $(typeof(T))

proc release*[T](x: var SharedPtr[T]) =
  echo "SharedPtr: release: ", $(typeof(T))
  x.decr()
  x.container = nil

proc `=destroy`*[T](x: var SharedPtr[T]) =
  if x.container != nil:
    echo "SharedPtr: destroy: ", x.container[].repr, " tp: ", $(typeof(T))
  decr(x)

proc `=dup`*[T](src: SharedPtr[T]): SharedPtr[T] =
  if src.container != nil and src.cnt != nil:
    discard atomicAddFetch(src.cnt, 1, ATOMIC_RELAXED)
  result.container = src.container
  result.cnt = src.cnt

proc `=copy`*[T](dest: var SharedPtr[T], src: SharedPtr[T]) =
  if src.container != nil:
    # echo "SharedPtr: copy: ", src.container.pointer.repr
    discard atomicAddFetch(addr src.container.cnt, 1, ATOMIC_RELAXED)
  `=destroy`(dest)
  dest.container = src.container

proc newSharedPtr*[T](val: sink Isolated[T]): SharedPtr[T] {.nodestroy.} =
  ## Returns a shared pointer which shares,
  ## ownership of the object by reference counting.
  result.container = cast[typeof(result.container)](allocShared(sizeof(result.container[])))
  result.container.cnt = 1
  result.container.value = extract val
  echo "SharedPtr: alloc: ", result.container[].repr, " tp: ", $(typeof(T))

template newSharedPtr*[T](val: T): SharedPtr[T] =
  newSharedPtr(isolate(val))

proc newSharedPtr*[T](t: typedesc[T]): SharedPtr[T] =
  ## Returns a shared pointer. It is not initialized,
  result.container = cast[typeof(result.container)](allocShared0(sizeof(result.container[])))
  result.cnt[] = 1
  echo "SharedPtr: alloc: ", result.container[].repr, " tp: ", $(typeof(T))

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
  if p.container == nil: "nil"
  else: $p.container[]
