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
    CatchableErr,
    DefectErr

  ThreadTypes* = void | bool | SomeInteger | DataBuffer | tuple | Atomic
  ThreadResErr* = (ErrorEnum, DataBuffer)
  ThreadResult*[T: ThreadTypes] = Result[T, ThreadResErr]


converter toThreadErr*(e: ref Exception): ThreadResErr {.inline, raises: [].} =
  if e of DatastoreKeyNotFound: (ErrorEnum.DatastoreKeyNotFoundErr, DataBuffer.new(e.msg))
  elif e of QueryEndedError: (ErrorEnum.QueryEndedErr, DataBuffer.new(e.msg))
  elif e of DatastoreError: (DatastoreErr, DataBuffer.new(e.msg))
  elif e of CatchableError: (CatchableErr, DataBuffer.new(e.msg))
  elif e of Defect: (DefectErr, DataBuffer.new(e.msg))
  else: raise (ref Defect)(msg: e.msg)

converter toExc*(e: ThreadResErr): ref CatchableError =
  case e[0]:
  of ErrorEnum.DatastoreKeyNotFoundErr: (ref DatastoreKeyNotFound)(msg: $e[1])
  of ErrorEnum.QueryEndedErr: (ref QueryEndedError)(msg: $e[1])
  of ErrorEnum.DatastoreErr: (ref DatastoreError)(msg: $e[1])
  of ErrorEnum.CatchableErr: (ref CatchableError)(msg: $e[1])
  of ErrorEnum.DefectErr: (ref CatchableError)(msg: "defect: " & $e[1])

proc toRes*(res: ThreadResult[void]): ?!void =
  res.mapErr() do(e: ThreadResErr) -> ref CatchableError:
    e.toExc()

proc toRes*[T,S](res: ThreadResult[T], m: proc(v: T): S = proc(v: T): T = v): ?!S =
  if res.isErr():
    result.err res.error().toExc()
  else:
    result.ok m(res.get())
