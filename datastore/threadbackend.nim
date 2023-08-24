import std/tables

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/upraises
import pkg/taskpools

import ./key
import ./query
import ./datastore
import ./databuffer

import fsds

export key, query

push: {.upraises: [].}

type
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

  ThreadDatastore* = ref object of Datastore
    tp: Taskpool

var backendDatastore {.threadvar.}: Datastore

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

proc has*(
  self: ThreadDatastore,
  key: KeyBuffer
): Future[?!bool] {.async.} =

  # without mounted =? self.dispatch(key):
  #   return failure "No mounted datastore found"
  # return (await mounted.store.store.has(mounted.relative))
  return success(true)

proc delete*(
  self: ThreadDatastore,
  key: KeyBuffer
): Future[?!void] {.async.} =

  # without mounted =? self.dispatch(key), error:
  #   return failure(error)
  # return (await mounted.store.store.delete(mounted.relative))
  return success()

proc delete*(
  self: ThreadDatastore,
  keys: seq[KeyBuffer]
): Future[?!void] {.async.} =

  # for key in keys:
  #   if err =? (await self.delete(key)).errorOption:
  #     return failure err

  return success()

proc get*(
  self: ThreadDatastore,
  key: KeyBuffer
): Future[?!DataBuffer] {.async.} =

  # without mounted =? self.dispatch(key), error:
  #   return failure(error)

  # return await mounted.store.store.get(mounted.relative)
  return success(DataBuffer.new())

proc put*(
  self: ThreadDatastore,
  key: KeyBuffer,
  data: DataBuffer
): Future[?!void] {.async.} =

  # without mounted =? self.dispatch(key), error:
  #   return failure(error)

  # return (await mounted.store.store.put(mounted.relative, data))
  return success()

proc put*(
  self: ThreadDatastore,
  batch: seq[BatchEntry]
): Future[?!void] {.async.} =

  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err

  return success()

proc close*(
  self: ThreadDatastore
): Future[?!void] {.async.} =
  self.tp.shutdown()
  return success()

func new*[S: ref Datastore](
  T: typedesc[ThreadDatastore],
  backend: ThreadBackend,
): ?!ThreadDatastore =

  var self = T()
  self.tp = Taskpool.new(num_threads = 1) ##\
    ## Default to one thread, multiple threads \
    ## will require more work

  let pending = self.tp.spawn startupDatastore(backend)
  sync pending

  success self
