
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
    val*: T
    err*: CatchableErrorBuffer

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

  ThreadDatastore* = object
    taskpool: Taskpool
    backendDatastore: Datastore

  ThreadDatastorePtr* = SharedPtr[ThreadDatastore]

proc new*[T](tp: typedesc[TResult[T]]): TResult[T] =
  newUniquePtr(ThreadResult[T])

proc startupDatastore(
    signal: ThreadSignalPtr,
    backend: ThreadBackend,
    ret: TResult[ThreadDatastorePtr],
): bool =
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
      let tds = newSharedPtr(ThreadDatastore)
      tds[].backendDatastore = ds.get()

      ret[].val = tds
      ret[].state = Success
    else:
      ret[].state = Error
  else:
    discard
  
  ret[].signal.fireSync().get()

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

func new*(
  T: typedesc[ThreadDatastore],
  signal: ThreadSignalPtr,
  backend: ThreadBackend,
  ret: TResult[ThreadDatastore]
) =

  var self = T()
  self.tp = Taskpool.new(num_threads = 1) ##\
    ## Default to one thread, multiple threads \
    ## will require more work

  let pending = self.tp.spawn startupDatastore(signal, backend)

