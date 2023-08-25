
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

import pretty

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
    NoBackend
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
    of NoBackend:
      discard

  ThreadDatastore* = object
    tp*: Taskpool
    backend*: ThreadBackendKind

  ThreadDatastorePtr* = SharedPtr[ThreadDatastore]

  Test* = object
    count*: ThreadBackendKind

  TestPtr* = SharedPtr[Test]

var
  fsDatastore {.threadvar.}: FSDatastore ##\
    ## TODO: figure out a better way to capture this?

proc newThreadResult*[T](tp: typedesc[T]): TResult[T] =
  newSharedPtr(ThreadResult[T])

proc startupDatastore(
    ret: TResult[ThreadDatastorePtr],
    backend: ThreadBackend,
    count: TestPtr,
) {.raises: [].} =
  ## starts up a FS instance on a give thread
  echo "\n"
  echo "\nstartupDatastore: threadId:", getThreadId()
  echo "\nstartupDatastore: ret:\n", ret.repr

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
      fsDatastore = ds.get()
      ret[].state = Success
    else:
      ret[].state = Error
      ret[].state = Success
  of TestBackend:
    echo "startupDatastore: TestBackend"
    ret[].value[].backend = TestBackend
    ret[].state = Success
  else:
    discard
  
  echo "startupDatastore: signal", ret[].signal.fireSync().get()

proc getTask*(
  ret: TResult[DataBuffer],
  backend: ThreadBackendKind,
  key: KeyBuffer,
) =
  # return ok(DataBuffer.new())
  print "\nthrbackend: getTask: ", ret[]
  print "\nthrbackend: getTask:key: ", key
  let data = DataBuffer.new("hello world!")
  print "\nthrbackend: getTask:data: ", data
  ret[].state = Success
  ret[].value = data

  print "thrbackend: putTask: fire", ret[].signal.fireSync().get()

proc get*(
  ret: TResult[DataBuffer],
  tds: ThreadDatastorePtr,
  key: Key,
) =
  echo "thrfrontend:put: "
  let bkey = StringBuffer.new(key.id())
  print "bkey: ", bkey

  tds[].tp.spawn getTask(ret, tds[].backend, bkey)

proc putTask*(
  ret: TResult[void],
  backend: ThreadBackendKind,
  key: KeyBuffer,
  data: DataBuffer,
) =
  print "\nthrbackend: putTask: ", ret[]
  print "\nthrbackend: putTask:key: ", key
  print "\nthrbackend: putTask:data: ", data

  print "thrbackend: putTask: fire", ret[].signal.fireSync().get()

proc put*(
  ret: TResult[void],
  tds: ThreadDatastorePtr,
  key: Key,
  data: seq[byte]
): TResult[void] =
  echo "thrfrontend:put: "
  let bkey = StringBuffer.new(key.id())
  let bval = DataBuffer.new(data)
  print "bkey: ", bkey
  print "bval: ", bval

  tds[].tp.spawn putTask(ret, tds[].backend, bkey, bval)

proc createThreadDatastore*(
  ret: var TResult[ThreadDatastorePtr],
  backend: ThreadBackend,
) =

  try:
    echo "createThreadDatastore: start"
    ret[].value[].tp = Taskpool.new(num_threads = 2)
    # echo "\n\ncreateThreadDatastore:tp:\n", ret[].repr
    echo "\n\ncreateThreadDatastore:value:\n", ret[].value.repr
    ret[].value[].tp.spawn startupDatastore(
      ret, backend, newSharedPtr(Test(count: FSBackend)))
    echo "createThreadDatastore: done"
    ret[].state = Success

  except Exception as exc:
    ret[].state = Error
    ret[].error = exc.toBuffer()
    discard

