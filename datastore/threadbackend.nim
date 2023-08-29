
import pkg/chronos/threadsync
import stew/results
import pkg/upraises
import pkg/taskpools

import ./key
import ./query
import ./datastore
import ./databuffer
import threading/smartptrs

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

  ThreadDatastore* = object
    tp*: Taskpool
    ds*: Datastore

  ThreadDatastorePtr* = SharedPtr[ThreadDatastore]

proc newThreadResult*[T](
    tp: typedesc[T]
): Result[TResult[T], ref CatchableError] =
  let res = newSharedPtr(ThreadResult[T])
  let signal = ThreadSignalPtr.new()
  if signal.isErr:
    return err((ref CatchableError)(msg: signal.error()))
  else:
    res[].signal = signal.get()
  ok res

proc getTask*(
  ret: TResult[DataBuffer],
  key: KeyBuffer,
) =
  # return ok(DataBuffer.new())
  print "\nthrbackend: getTask: ", ret[]
  print "\nthrbackend: getTask:key: ", key
  let data = DataBuffer.new("hello world!")
  print "\nthrbackend: getTask:data: ", data
  ret[].state = Success
  ret[].value = data

  print "thrbackend: putTask: fire", ret[].signal.fireSync()

proc get*(
  ret: TResult[DataBuffer],
  tds: ThreadDatastorePtr,
  key: Key,
) =
  echo "thrfrontend:put: "
  let bkey = StringBuffer.new(key.id())
  print "bkey: ", bkey

  tds[].tp.spawn getTask(ret, bkey)

import os

proc putTask*(
  ret: TResult[void],
  key: KeyBuffer,
  data: DataBuffer,
) =
  print "\nthrbackend: putTask: ", ret[]
  print "\nthrbackend: putTask:key: ", key
  print "\nthrbackend: putTask:data: ", data

  os.sleep(200)
  print "thrbackend: putTask: fire", ret[].signal.fireSync().get()

proc put*(
  ret: TResult[void],
  tds: ThreadDatastorePtr,
  key: Key,
  data: seq[byte]
) =
  echo "thrfrontend:put: "
  let bkey = StringBuffer.new(key.id())
  let bval = DataBuffer.new(data)
  print "bkey: ", bkey
  print "bval: ", bval

  tds[].tp.spawn putTask(ret, bkey, bval)
