import std/os
import std/options
import std/strutils

import pkg/questionable
import pkg/questionable/results
from pkg/stew/results as stewResults import get, isErr
import pkg/upraises

import ./backend
import ./datastore

export datastore

push: {.upraises: [].}

type
  FSDatastore* = object
    root*: DataBuffer
    ignoreProtected: bool
    depth: int

proc validDepth*(self: FSDatastore, key: Key): bool =
  key.len <= self.depth

proc isRootSubdir*(root, path: string): bool =
  path.startsWith(root)

proc path*(self: FSDatastore, key: Key): ?!string =
  ## Return filename corresponding to the key
  ## or failure if the key doesn't correspond to a valid filename
  ##

  if not self.validDepth(key):
    return failure "Path has invalid depth!"

  var
    segments: seq[string]

  for ns in key:
    let basename = ns.value.extractFilename
    if basename == "" or not basename.isValidFilename:
      return failure "Filename contains invalid chars!"

    if ns.field == "":
      segments.add(ns.value)
    else:
      let basename = ns.field.extractFilename
      if basename == "" or not basename.isValidFilename:
        return failure "Filename contains invalid chars!"

      # `:` are replaced with `/`
      segments.add(ns.field / ns.value)

  let
    root = $self.root
    fullname = (root / segments.joinPath())
      .absolutePath()
      .catch()
      .get()
      .addFileExt(FileExt)

  if not root.isRootSubdir(fullname):
    return failure "Path is outside of `root` directory!"

  return success fullname

proc has*(self: FSDatastore, key: KeyId): ?!bool =
  let key = key.toKey()
  return self.path(key).?fileExists()

proc delete*(self: FSDatastore, key: KeyId): ?!void =
  let key = key.toKey()

  without path =? self.path(key), error:
    return failure error

  if not path.fileExists():
    return success()

  try:
    removeFile(path)
  except OSError as e:
    return failure e

  return success()

proc delete*(self: FSDatastore, keys: openArray[KeyId]): ?!void =
  for key in keys:
    if err =? self.delete(key).errorOption:
      return failure err

  return success()

proc readFile*[V](self: FSDatastore, path: string): ?!V =
  var
    file: File

  defer:
    file.close

  if not file.open(path):
    return failure "unable to open file!"

  try:
    let
      size = file.getFileSize().int

    when V is seq[byte]:
      var bytes = newSeq[byte](size)
    elif V is DataBuffer:
      var bytes = DataBuffer.new(capacity=size)
    else:
      {.error: "unhandled result type".}
    var
      read = 0

    while read < size:
      read += file.readBytes(bytes.toOpenArray(), read, size)

    if read < size:
      return failure $read & " bytes were read from " & path &
        " but " & $size & " bytes were expected"

    return success bytes

  except CatchableError as e:
    return failure e

proc get*(self: FSDatastore, key: KeyId): ?!DataBuffer =
  let key = key.toKey()
  without path =? self.path(key), error:
    return failure error

  if not path.fileExists():
    return failure(
      newException(DatastoreKeyNotFound, "Key doesn't exist"))

  return readFile[DataBuffer](self, path)

proc put*(
  self: FSDatastore,
  key: KeyId,
  data: DataBuffer): ?!void =

  without path =? self.path(key), error:
    return failure error

  try:
    createDir(parentDir(path))
    writeFile(path, data)
  except CatchableError as e:
    return failure e

  return success()

proc put*(
  self: FSDatastore,
  batch: seq[BatchEntry]): ?!void =

  for entry in batch:
    if err =? self.put(entry.key, entry.data).errorOption:
      return failure err

  return success()

proc dirWalker(path: string): iterator: string {.gcsafe.} =
  var localPath {.threadvar.}: string

  localPath = path
  return iterator(): string =
    try:
      for p in path.walkDirRec(yieldFilter = {pcFile}, relative = true):
        yield p
    except CatchableError as exc:
      raise newException(Defect, exc.msg)

proc close*(self: FSDatastore): ?!void =
  return success()

proc query*(
  self: FSDatastore,
  query: DbQuery[KeyId, DataBuffer]): ?!QueryIter =

  without path =? self.path(query.key), error:
    return failure error

  let basePath =
    # it there is a file in the directory
    # with the same name then list the contents
    # of the directory, otherwise recurse
    # into subdirectories
    if path.fileExists:
      path.parentDir
    else:
      path.changeFileExt("")

  let
    walker = dirWalker(basePath)

  var
    iter = QueryIter.new()

  # var lock = newAsyncLock() # serialize querying under threads
  proc next(): ?!QueryResponse =
    # defer:
    #   if lock.locked:
    #     lock.release()

    # if lock.locked:
    #   return failure (ref DatastoreError)(msg: "Should always await query features")

    let
      root = $self.root
      path = walker()

    if iter.finished:
      return failure "iterator is finished"

    # await lock.acquire()

    if finished(walker):
      iter.finished = true
      return success (Key.none, EmptyBytes)

    var
      keyPath = basePath

    keyPath.removePrefix(root)
    keyPath = keyPath / path.changeFileExt("")
    keyPath = keyPath.replace("\\", "/")

    let
      key = Key.init(keyPath).expect("should not fail")
      data =
        if query.value:
          self.readFile[DataBuffer]((basePath / path).absolutePath)
            .expect("Should read file")
        else:
          @[]

    return success (key.some, data)

  iter.next = next
  return success iter

proc new*(
  T: type FSDatastore,
  root: string,
  depth = 2,
  caseSensitive = true,
  ignoreProtected = false): ?!T =

  let root = ? (
    block:
      if root.isAbsolute: root
      else: getCurrentDir() / root).catch

  if not dirExists(root):
    return failure "directory does not exist: " & root

  success T(
    root: root,
    ignoreProtected: ignoreProtected,
    depth: depth)
