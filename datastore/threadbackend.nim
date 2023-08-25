
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

export key, query, smartptrs, databuffer

push: {.upraises: [].}

type

  ThreadResultKind* {.pure.} = enum
    NotReady
    Success
    Error

  ThreadResult*[T: DataBuffer | void] = object
    state*: ThreadResultKind
    signal*: ThreadSignalPtr
    value*: T
    error*: CatchableErrorBuffer

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

  ThreadDatastore = object
    tp: Taskpool
    backendDatastore: Datastore

  ThreadDatastorePtr* = SharedPtr[ThreadDatastore]

proc newThreadResult*[T](tp: typedesc[T]): UniquePtr[ThreadResult[T]] =
  newUniquePtr(ThreadResult[T])

proc startupDatastore(
    ret: TResult[ThreadDatastorePtr],
    backend: ThreadBackend,
) {.raises: [].} =
  ## starts up a FS instance on a give thread
  case backend.kind:
  of FSBackend:
    let ds = FSDatastore.new(
      root = backend.root.toString(),
      depth = backend.depth,
      caseSensitive = backend.caseSensitive,
      ignoreProtected = backend.ignoreProtected
    )
    if ds.isOk:
      ret[].value[].backendDatastore = ds.get()
      ret[].state = Success
    else:
      ret[].state = Error
  else:
    discard
  
  discard ret[].signal.fireSync().get()

proc getTask*(
  self: ThreadDatastorePtr,
  key: KeyBuffer,
  ret: TResult[DataBuffer]
) =
  # return ok(DataBuffer.new())
  discard

proc putTask*(
  self: ThreadDatastorePtr,
  key: KeyBuffer,
  data: DataBuffer,
  ret: TResult[void]
) =
  discard

# proc close*(
#   self: ThreadDatastore,
#   signal: ThreadSignalPtr,
# ): TResult[void] =
#   try:
#     self[].tp.shutdown()
#     return ok()
#   except Exception as exc:
#     return TResult[void].new()

proc createThreadDatastore*(
  ret: TResult[ThreadDatastorePtr],
  backend: ThreadBackend,
) =

  try:
    ret[].value[].tp = Taskpool.new(num_threads = 1) ##\
    ## Default to one thread, multiple threads \
    ## will require more work
    ret[].value[].tp.spawn startupDatastore(ret, backend)

  except Exception as exc:
    ret[].state = Error
    ret[].error = exc.toBuffer()

