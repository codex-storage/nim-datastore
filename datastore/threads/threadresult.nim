import std/atomics
import std/options

import pkg/questionable/results
import pkg/results

import ../types
import ../query
import ../key

import ../backend
import ./databuffer

type
  ErrorEnum* {.pure.} = enum
    DatastoreErr,
    DatastoreKeyNotFoundErr,
    QueryEndedErr,
    CatchableErr

  ThreadTypes* = void | bool | SomeInteger | DataBuffer | tuple | Atomic
  ThreadResErr* = (ErrorEnum, DataBuffer)
  # ThreadQueryRes* = tuple[key: KeyId, val: DataBuffer]
  ThreadResult*[T: ThreadTypes] = Result[T, ThreadResErr]


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

# converter toQueryResponse*(r: DbQueryResponse): QueryResponse =
#   if not r.key.data.isNil and r.key.data.len > 0 and key =? Key.init($r.key.data):
#     (key.some, @(r.val))
#   else:
#     (Key.none, EmptyBytes)
