
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
    TestBackend
    FSBackend
    SQliteBackend

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

  ThreadDatastore* = object
    tp: Taskpool
    backendDatastore: ThreadBackendKind

  ThreadDatastorePtr* = UniquePtr[ThreadDatastore]

  Test* = object
    count*: int

  TestPtr* = SharedPtr[Test]

var fsBackend {.threadvar.}: FSDatastore

proc newThreadResult*[T](tp: typedesc[T]): TResult[T] =
  newSharedPtr(ThreadResult[T])

proc startupDatastore(
    ret: TResult[ThreadDatastorePtr],
    backend: ThreadBackend,
    count: TestPtr,
) {.raises: [].} =
  ## starts up a FS instance on a give thread
  var
    ret: TResult[ThreadDatastorePtr]
    backend: ThreadBackend
  echo "\n\nstartupDatastore: ret:\n", ret.repr

  echo "\nstartupDatastore: backend:\n", backend.repr
  echo "\nstartupDatastore: count:\n", count.repr

  echo ""
  case backend.kind:
  of FSBackend:
    let ds = FSDatastore.new(
      root = backend.root.toString(),
      depth = backend.depth,
      caseSensitive = backend.caseSensitive,
      ignoreProtected = backend.ignoreProtected
    )
    if ds.isOk:
      fsBackend = ds.get()
      ret[].state = Success
    else:
      ret[].state = Error
      ret[].error = newException(DatastoreError, "error creating signal").toBuffer()
  of TestBackend:
    echo "startupDatastore: TestBackend"
    ret[].state = Success
  else:
    discard
  
  echo "startupDatastore: signal"
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

proc createThreadDatastore*(
  ret: TResult[ThreadDatastorePtr],
  backend: ThreadBackend,
) =

  try:
    echo "createThreadDatastore: start"
    ret[].value[].tp = Taskpool.new(num_threads = 2)
    echo "\n\ncreateThreadDatastore:tp:\n", ret[].repr
    ret[].value[].tp.spawn startupDatastore(
      ret, backend, newSharedPtr(Test(count: 22)))
    echo "createThreadDatastore: done"
    ret[].state = Success

  except Exception as exc:
    ret[].state = Error
    ret[].error = exc.toBuffer()
    discard

