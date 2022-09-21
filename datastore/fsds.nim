import std/os
import std/options
import std/strutils

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
from pkg/stew/results as stewResults import get, isErr
import pkg/upraises

import ./datastore

export datastore

push: {.upraises: [].}

type
  FSDatastore* = ref object of Datastore
    root*: string
    ignoreProtected: bool
    depth: int

template path*(self: FSDatastore, key: Key): string =
  var
    segments: seq[string]

  for ns in key:
    if ns.field == "":
      segments.add ns.value
      continue

    # `:` are replaced with `/`
    segments.add(ns.field / ns.value)

  (self.root / segments.joinPath()).absolutePath()

template validDepth*(self: FSDatastore, key: Key): bool =
  key.len <= self.depth

template isRootSubdir*(self: FSDatastore, path: string): bool =
  path.startsWith(self.root)

method contains*(self: FSDatastore, key: Key): Future[?!bool] {.async.} =

  if not self.validDepth(key):
    return failure "Path has invalid depth!"

  let
    path = self.path(key).addFileExt(FileExt)

  if not self.isRootSubdir(path):
    return failure "Path is outside of `root` directory!"

  if not path.extractFilename.isValidFilename:
    return failure "Filename contains invalid chars!"

  return success path.fileExists()

method delete*(self: FSDatastore, key: Key): Future[?!void] {.async.} =

  if not self.validDepth(key):
    return failure "Path has invalid depth!"

  let
    path = self.path(key).addFileExt(FileExt)

  if not self.isRootSubdir(path):
    return failure "Path is outside of `root` directory!"

  if not path.extractFilename.isValidFilename:
    return failure "Filename contains invalid chars!"

  if not path.fileExists():
    return failure newException(DatastoreKeyNotFound, "Key not found!")

  try:
    removeFile(path)
    return success()
  except OSError as e:
    return failure e

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

  if not self.validDepth(key):
    return failure "Path has invalid depth!"

  let
    path = self.path(key).addFileExt(FileExt)

  if not self.isRootSubdir(path):
    return failure "Path is outside of `root` directory!"

  if not path.fileExists():
    return failure(newException(DatastoreKeyNotFound, "Key doesn't exist"))

  return self.readFile(path)

method put*(
  self: FSDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async, locks: "unknown".} =

  if not self.validDepth(key):
    return failure "Path has invalid depth!"

  let
    path = self.path(key)

  if not self.isRootSubdir(path):
    return failure "Path is outside of `root` directory!"

  if not path.extractFilename.isValidFilename:
    return failure "Filename contains invalid chars!"

  try:
    createDir(parentDir(path))
    writeFile(path.addFileExt(FileExt), data)
  except CatchableError as e:
    return failure e

  return success()

proc dirWalker(path: string): iterator: string {.gcsafe.} =
  return iterator(): string =
    try:
      for p in path.walkDirRec(yieldFilter = {pcFile}, relative = true):
        yield p
    except CatchableError as exc:
      raise newException(Defect, exc.msg)

method query*(
  self: FSDatastore,
  query: Query): Future[?!QueryIter] {.async.} =
  var
    iter = QueryIter.new()

  let
    basePath = self.path(query.key).parentDir
    walker = dirWalker(basePath)

  proc next(): Future[?!QueryResponse] {.async.} =
    let
      path = walker()

    if finished(walker):
      iter.finished = true
      return success (Key.none, EmptyBytes)

    without data =? self.readFile((basePath / path).absolutePath), err:
      return failure err

    var
      keyPath = basePath

    keyPath.removePrefix(self.root)
    keyPath = keyPath / path.changeFileExt("")
    keyPath = keyPath.replace("\\", "/")

    let
      key = Key.init(keyPath).expect("should not fail")

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
