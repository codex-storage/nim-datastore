
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

  ThreadResult*[T: DataBuffer | void | ThreadDatastorePtr] = object
    state*: ThreadResultKind
    signal*: ThreadSignalPtr
    value*: T
    error*: CatchableErrorBuffer

  TResult*[T] = SharedPtr[ThreadResult[T]]

  ThreadBackendKind* {.pure.} = enum
    FSBackend
    SQliteBackend
    TestBackend

  ThreadBackend* = object
    case kind*: ThreadBackendKind
    of FSBackend:
      root*: StringBuffer
      depth*: int
      caseSensitive*: bool
      ignoreProtected*: bool
    of SQliteBackend:
      discard
    of TestBackend:
      count*: int

  ThreadDatastore = object
    tp: Taskpool
    backendDatastore: Datastore

  ThreadDatastorePtr* = SharedPtr[ThreadDatastore]

proc newThreadResult*[T](tp: typedesc[T]): TResult[T] =
  newSharedPtr(ThreadResult[T])

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
      ret[].value[].backendDatastore = ds.get()
      ret[].state = Success
  of TestBackend:
    echo "startupDatastore: TestBackend"
    ret[].value[].backendDatastore = nil
    ret[].state = Success
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
    echo "createThreadDatastore: start"
    ret[].value[].tp = Taskpool.new(num_threads = 2) ##\
    ## Default to one thread, multiple threads \
    ## will require more work
    ret[].value[].tp.spawn startupDatastore(ret, backend)
    echo "createThreadDatastore: done"

  except Exception as exc:
    ret[].state = Error
    ret[].error = exc.toBuffer()

