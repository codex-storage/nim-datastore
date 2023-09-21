import std/atomics
import std/options

import pkg/questionable/results
import pkg/results

import ../types
import ../query
import ../key

import ./databuffer

type
  ErrorEnum* {.pure.} = enum
    DatastoreErr,
    DatastoreKeyNotFoundErr,
    QueryEndedErr,
    CatchableErr

  ThreadTypes* = void | bool | SomeInteger | DataBuffer | tuple | Atomic
  ThreadResErr* = (ErrorEnum, DataBuffer)
  ThreadQueryRes* = (DataBuffer, DataBuffer)
  ThreadResult*[T: ThreadTypes] = Result[T, ThreadResErr]

  DbKey* = tuple[data: DataBuffer]
  DbValue* = tuple[data: DataBuffer]

proc toDb*(key: Key): DbKey {.inline, raises: [].} =
  (data: DataBuffer.new(key.id()))

proc toKey*(key: DbKey): Key {.inline, raises: [].} =
  Key.init(key.data).expect("expected valid key here for but got `" & $key.data & "`")

proc toDb*(value: sink seq[byte]): DbValue {.inline, raises: [].} =
  (data: DataBuffer.new(value))

proc toValue*(value: DbValue): seq[byte] {.inline, raises: [].} =
  value.data.toSeq()

template toOpenArray*(x: DbKey): openArray[char] =
  x.data[].buf.toOpenArray(0, x[].size-1)

converter toThreadErr*(e: ref CatchableError): ThreadResErr {.inline, raises: [].} =
  if e of DatastoreKeyNotFound: (ErrorEnum.DatastoreKeyNotFoundErr, DataBuffer.new(e.msg))
  elif e of QueryEndedError: (ErrorEnum.QueryEndedErr, DataBuffer.new(e.msg))
  elif e of DatastoreError: (DatastoreErr, DataBuffer.new(e.msg))
  elif e of CatchableError: (CatchableErr, DataBuffer.new(e.msg))
  else: raise (ref Defect)(msg: e.msg)

converter toExc*(e: ThreadResErr): ref CatchableError =
  case e[0]:
  of ErrorEnum.DatastoreKeyNotFoundErr: (ref DatastoreKeyNotFound)(msg: $e[1])
  of ErrorEnum.QueryEndedErr: (ref QueryEndedError)(msg: $e[1])
  of ErrorEnum.DatastoreErr: (ref DatastoreError)(msg: $e[1])
  of ErrorEnum.CatchableErr: (ref CatchableError)(msg: $e[1])

converter toQueryResponse*(r: ThreadQueryRes): QueryResponse =
  if not r[0].isNil and r[0].len > 0 and key =? Key.init($r[0]):
    (key.some, @(r[1]))
  else:
    (Key.none, EmptyBytes)
