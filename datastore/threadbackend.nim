
import pkg/chronos/threadsync
import stew/results
import pkg/upraises
import pkg/taskpools

import ./key
import ./query
import ./datastore
import ./databuffer
import threading/smartptrs

import fsds

export key, query

push: {.upraises: [].}

type

  ThreadResult*[T: DataBuffer | void] = Result[T, CatchableErrorBuffer]

  TResult*[T] = UniquePtr[ThreadResult[T]]

  ThreadBackendKind* {.pure.} = enum
    FSBackend
    SQliteBackend

  ThreadBackend* = object
    case kind*: ThreadBackendKind
    of FSBackend:
      root: StringBuffer
      depth: int
      caseSensitive: bool
      ignoreProtected: bool
    of SQliteBackend:
      discard

  ThreadDatastore* = ptr object
    tp: Taskpool

var backendDatastore {.threadvar.}: Datastore

proc new*[T](tp: typedesc[TResult[T]]): TResult[T] =
  newUniquePtr(ThreadResult[T])

proc startupDatastore(backend: ThreadBackend): bool =
  ## starts up a FS instance on a give thread
  case backend.kind:
  of FSBackend:
    let res = FSDatastore.new(
      root = backend.root.toString(),
      depth = backend.depth,
      caseSensitive = backend.caseSensitive,
      ignoreProtected = backend.ignoreProtected)
    if res.isOk:
      backendDatastore =  res.get()
  else:
    discard

proc get*(
  self: ThreadDatastore,
  signal: ThreadSignalPtr,
  key: KeyBuffer
): Result[DataBuffer, CatchableErrorBuffer] =

  return ok(DataBuffer.new())

proc put*(
  self: ThreadDatastore,
  signal: ThreadSignalPtr,
  key: KeyBuffer,
  data: DataBuffer,
): TResult[void] =

  return TResult[void].new()

proc close*(
  self: ThreadDatastore,
  signal: ThreadSignalPtr,
): Result[void, CatchableErrorBuffer] =
  try:
    self[].tp.shutdown()
    return ok()
  except Exception as exc:
    return err(exc.toBuffer())

func new*[S: ref Datastore](
  T: typedesc[ThreadDatastore],
  signal: ThreadSignalPtr,
  backend: ThreadBackend,
): Result[ThreadDatastore, CatchableErrorBuffer] =

  var self = T()
  self.tp = Taskpool.new(num_threads = 1) ##\
    ## Default to one thread, multiple threads \
    ## will require more work

  let pending = self.tp.spawn startupDatastore(backend)
  sync pending

  ok self
