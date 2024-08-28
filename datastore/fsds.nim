{.push raises: [].}

import std/os
import std/options
import std/tables
import std/strutils

import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./defaultimpl
import ./datastore

export datastore

type
  FSDatastore* = ref object of Datastore
    root*: string
    ignoreProtected: bool
    depth: int
    locks: TableRef[Key, AsyncLock]

proc validDepth*(self: FSDatastore, key: Key): bool =
  key.len <= self.depth

proc isRootSubdir*(self: FSDatastore, path: string): bool =
  path.startsWith(self.root)

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
    fullname = (self.root / segments.joinPath())
      .absolutePath()
      .catch()
      .get()
      .addFileExt(FileExt)

  if not self.isRootSubdir(fullname):
    return failure "Path is outside of `root` directory!"

  return success fullname

method has*(self: FSDatastore, key: Key): Future[?!bool] {.async.} =
  return self.path(key).?fileExists()

method delete*(self: FSDatastore, key: Key): Future[?!void] {.async.} =
  without path =? self.path(key), error:
    return failure error

  if not path.fileExists():
    return success()

  try:
    removeFile(path)
  except OSError as e:
    return failure e

  return success()

method delete*(self: FSDatastore, keys: seq[Key]): Future[?!void] {.async.} =
  for key in keys:
    if err =? (await self.delete(key)).errorOption:
      return failure err

  return success()

proc readFile*(self: FSDatastore, path: string): ?!seq[byte] =
  var
    file: File

  defer:
    file.close

  if not file.open(path):
    return failure "unable to open file!"

  try:
    let
      size = file.getFileSize

    var
      bytes = newSeq[byte](size)
      read = 0

    while read < size:
      read += file.readBytes(bytes, read, size)

    if read < size:
      return failure $read & " bytes were read from " & path &
        " but " & $size & " bytes were expected"

    return success bytes

  except CatchableError as e:
    return failure e

method get*(self: FSDatastore, key: Key): Future[?!seq[byte]] {.async.} =
  without path =? self.path(key), error:
    return failure error

  if not path.fileExists():
    return failure(
      newException(DatastoreKeyNotFound, "Key doesn't exist"))

  return self.readFile(path)

method put*(
  self: FSDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async.} =

  without path =? self.path(key), error:
    return failure error

  try:
    createDir(parentDir(path))
    writeFile(path, data)
  except CatchableError as e:
    return failure e

  return success()

method put*(
  self: FSDatastore,
  batch: seq[BatchEntry]): Future[?!void] {.async.} =

  for entry in batch:
    if err =? (await self.put(entry.key, entry.data)).errorOption:
      return failure err

  return success()

proc dirWalker(path: string): (iterator: string {.raises: [Defect], gcsafe.}) =
  return iterator(): string =
    try:
      for p in path.walkDirRec(yieldFilter = {pcFile}, relative = true):
        yield p
    except CatchableError as exc:
      raise newException(Defect, exc.msg)

method close*(self: FSDatastore): Future[?!void] {.async.} =
  return success()

method query*(
  self: FSDatastore,
  query: Query): Future[?!QueryIter] {.async.} =

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

  proc next(): Future[?!QueryResponse] {.async.} =
    if iter.finished:
      return success (Key.none, EmptyBytes)

    let
      path = walker()

    if finished(walker):
      iter.finished = true
      return success (Key.none, EmptyBytes)

    var
      keyPath = basePath

    keyPath.removePrefix(self.root)
    keyPath = keyPath / path.changeFileExt("")
    keyPath = keyPath.replace("\\", "/")

    let
      key = Key.init(keyPath).expect("should not fail")
      data =
        if query.value:
          self.readFile((basePath / path).absolutePath)
            .expect("Should read file")
        else:
          @[]

    return success (key.some, data)

  iter.next = next
  iter.dispose = proc(): Future[?!void] {.async.} =
    iter.finished = true
    return success()
  
  return success iter

method modifyGet*(
  self: FSDatastore,
  key: Key,
  fn: ModifyGet): Future[?!seq[byte]] {.async.} =
  var lock: AsyncLock
  try:
    lock = self.locks.mgetOrPut(key, newAsyncLock())
    return await defaultModifyGetImpl(self, lock, key, fn)
  finally:
    if not lock.locked:
      self.locks.del(key)

method modify*(
  self: FSDatastore,
  key: Key,
  fn: Modify): Future[?!void] {.async.} =
  var lock: AsyncLock
  try:
    lock = self.locks.mgetOrPut(key, newAsyncLock())
    return await defaultModifyImpl(self, lock, key, fn)
  finally:
    if not lock.locked:
      self.locks.del(key)

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
    depth: depth,
    locks: newTable[Key, AsyncLock]()
    )
