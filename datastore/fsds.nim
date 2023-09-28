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
  FSDatastore*[K, V] = object
    root*: DataBuffer
    ignoreProtected: bool
    depth: int

proc isRootSubdir*(root, path: string): bool =
  path.startsWith(root)

proc validDepth(self: FSDatastore, key: Key): bool =
  key.len <= self.depth

proc findPath*[K,V](self: FSDatastore[K,V], key: K): ?!string =
  ## Return filename corresponding to the key
  ## or failure if the key doesn't correspond to a valid filename
  ##
  let root = $self.root
  let key = Key.init($key).get()
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
    fullname = (root / segments.joinPath())
      .absolutePath()
      .catch()
      .get()
      .addFileExt(FileExt)

  if not root.isRootSubdir(fullname):
    return failure "Path is outside of `root` directory!"

  return success fullname

proc has*[K,V](self: FSDatastore[K,V], key: K): ?!bool =
  without path =? self.findPath(key), error:
    return failure error
  success path.fileExists()

proc contains*[K](self: FSDatastore, key: K): bool =
  return self.has(key).get()

proc delete*[K,V](self: FSDatastore[K,V], key: K): ?!void =
  without path =? self.findPath(key), error:
    return failure error

  if not path.fileExists():
    return success()

  try:
    removeFile(path)
  except OSError as e:
    return failure e

  return success()

proc delete*[K,V](self: FSDatastore[K,V], keys: openArray[K]): ?!void =
  for key in keys:
    if err =? self.delete(key).errorOption:
      return failure err

  return success()

proc readFile[V](self: FSDatastore, path: string): ?!V =
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
      var bytes = DataBuffer.new(size=size)
    else:
      {.error: "unhandled result type".}
    var
      read = 0

    echo "BYTES: ", bytes.repr
    while read < size:
      read += file.readBytes(bytes.toOpenArray(), read, size)

    if read < size:
      return failure $read & " bytes were read from " & path &
        " but " & $size & " bytes were expected"

    return success bytes

  except CatchableError as e:
    return failure e

proc get*[K,V](self: FSDatastore[K,V], key: K): ?!DataBuffer =
  without path =? self.findPath(key), error:
    return failure error

  if not path.fileExists():
    return failure(
      newException(DatastoreKeyNotFound, "Key doesn't exist"))

  return readFile[DataBuffer](self, path)

proc put*[K,V](
  self: FSDatastore[K,V],
  key: KeyId,
  data: DataBuffer): ?!void =

  without path =? self.findPath(key), error:
    return failure error

  try:
    var data = data
    createDir(parentDir(path))
    writeFile(path, data.toOpenArray())
  except CatchableError as e:
    return failure e

  return success()

proc put*[K,V](
  self: FSDatastore,
  batch: seq[DbBatchEntry[K, V]]): ?!void =

  for entry in batch:
    if err =? self.put(entry.key, entry.data).errorOption:
      return failure err

  return success()

iterator dirIter(path: string): string {.gcsafe.} =
  try:
    for p in path.walkDirRec(yieldFilter = {pcFile}, relative = true):
      yield p
  except CatchableError as exc:
    raise newException(Defect, exc.msg)

proc close*[K,V](self: FSDatastore[K,V]): ?!void =
  return success()

type
  FsQueryEnv*[K,V] = tuple[self: FSDatastore[K,V], basePath: DataBuffer]

proc query*(
  self: FSDatastore,
  query: DbQuery[KeyId],
): Result[DbQueryHandle[KeyId, DataBuffer, FsQueryEnv], ref CatchableError] =

  let key = query.key.toKey()
  without path =? self.findPath(key), error:
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
  
  let env: FsQueryEnv = (self: self, basePath: DataBuffer.new(basePath))
  success DbQueryHandle[KeyId, DataBuffer, FsQueryEnv](env: env)

iterator iter*[K, V](handle: var DbQueryHandle[K, V, DataBuffer]): ?!DbQueryResponse[K, V] =
  let root = $(handle.env)

  for path in root.dirIter():
    if handle.cancel:
      return

    var keyPath = handle.basePath

    keyPath.removePrefix(root)
    keyPath = keyPath / path.changeFileExt("")
    keyPath = keyPath.replace("\\", "/")

    let
      fl = (handle.env.basePath / path).absolutePath()
      key = Key.init(keyPath).expect("should not fail")
      data =
        if query.value:
          let res = readFile[DataBuffer](handle.env.self, fl)
          if res.isErr():
            yield failure res.error()
          res.get()
        else:
          DataBuffer.new()

    yield success (key.some, data)

proc newFSDatastore*[K,V](root: string,
                          depth = 2,
                          caseSensitive = true,
                          ignoreProtected = false
                         ): ?!FSDatastore[K,V] =

  let root = ? (
    block:
      if root.isAbsolute: root
      else: getCurrentDir() / root).catch

  if not dirExists(root):
    return failure "directory does not exist: " & root

  success FSDatastore[K,V](
    root: DataBuffer.new root,
    ignoreProtected: ignoreProtected,
    depth: depth)
