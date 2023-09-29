import std/os
import std/options
import std/strutils
import std/tempfiles

import pkg/questionable
import pkg/questionable/results
from pkg/stew/results as stewResults import get, isErr
import pkg/upraises

import ./backend

export backend

push: {.upraises: [].}

import std/sharedtables

type
  KeyLock = tuple[locked: bool]

var keyTable: SharedTable[KeyId, KeyLock]
keyTable.init()

template lockKeyImpl(key: KeyId, blk: untyped) =
  var hasLock = false
  try:
    while not hasLock:
      keyTable.withKey(key) do (k: KeyId, klock: var KeyLock, exists: var bool):
        if not exists or not klock.locked:
          klock.locked = true
        exists = true
        hasLock = klock.locked
      os.sleep(1)

    `blk`
  finally:
    if hasLock:
      keyTable.withKey(key) do (k: KeyId, klock: var KeyLock, exists: var bool):
        assert exists and klock.locked
        klock.locked = false
        exists = false

template withReadLock(key: KeyId, blk: untyped) =
  lockKeyImpl(key, blk)

template withWriteLock(key: KeyId, blk: untyped) =
  lockKeyImpl(key, blk)

type
  FSBackend*[K, V] = object
    root*: DataBuffer
    ignoreProtected: bool
    depth*: int

proc isRootSubdir*(root, path: string): bool =
  path.startsWith(root)

proc validDepth*(self: FSBackend, key: Key): bool =
  key.len <= self.depth

proc findPath*[K,V](self: FSBackend[K,V], key: K): ?!string =
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

proc has*[K,V](self: FSBackend[K,V], key: K): ?!bool =
  without path =? self.findPath(key), error:
    return failure error
  withReadLock(key):
    success path.fileExists()

proc contains*[K](self: FSBackend, key: K): bool =
  return self.has(key).get()

proc delete*[K,V](self: FSBackend[K,V], key: K): ?!void =
  without path =? self.findPath(key), error:
    return failure error

  if not path.fileExists():
    return success()

  try:
    withWriteLock(key):
      removeFile(path)
  except OSError as e:
    return failure e

  return success()

proc delete*[K,V](self: FSBackend[K,V], keys: openArray[K]): ?!void =
  for key in keys:
    if err =? self.delete(key).errorOption:
      return failure err

  return success()

proc readFile[V](self: FSBackend, path: string): ?!V =
  var
    file: File

  defer:
    file.close

  withReadLock(key):
    if not file.open(path):
      return failure "unable to open file! path: " & path

    try:
      let
        size = file.getFileSize().int

      when V is seq[byte]:
        var bytes = newSeq[byte](size)
      elif V is V:
        var bytes = V.new(size=size)
      else:
        {.error: "unhandled result type".}
      var
        read = 0

      # echo "BYTES: ", bytes.repr
      while read < size:
        read += file.readBytes(bytes.toOpenArray(0, size-1), read, size)

      if read < size:
        return failure $read & " bytes were read from " & path &
          " but " & $size & " bytes were expected"

      return success bytes

    except CatchableError as e:
      return failure e

proc get*[K,V](self: FSBackend[K,V], key: K): ?!V =
  without path =? self.findPath(key), error:
    return failure error

  if not path.fileExists():
    return failure(
      newException(DatastoreKeyNotFound, "Key doesn't exist"))

  return readFile[V](self, path)

proc put*[K,V](self: FSBackend[K,V],
               key: K,
               data: V
              ): ?!void =

  without path =? self.findPath(key), error:
    return failure error

  try:
    var data = data
    withWriteLock(KeyId.new path):
      createDir(parentDir(path))

    let tmpPath = genTempPath("temp", "", path.splitPath.tail)
    writeFile(tmpPath, data.toOpenArray(0, data.len()-1))

    withWriteLock(key):
      moveFile(tmpPath, path)
  except CatchableError as e:
    return failure e

  return success()

proc put*[K,V](
  self: FSBackend,
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

proc close*[K,V](self: FSBackend[K,V]): ?!void =
  return success()

type
  FsQueryHandle*[K, V] = object
    query*: DbQuery[K]
    cancel*: bool
    closed*: bool
    env*: FsQueryEnv[K,V]

  FsQueryEnv*[K,V] = object
    self: FSBackend[K,V]
    basePath: DataBuffer

proc query*[K,V](
  self: FSBackend[K,V],
  query: DbQuery[K],
): Result[FsQueryHandle[K, V], ref CatchableError] =

  let key = query.key
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
  
  let env = FsQueryEnv[K,V](self: self, basePath: DataBuffer.new(basePath))
  success FsQueryHandle[K, V](query: query, env: env)

proc close*[K,V](handle: var FsQueryHandle[K,V]) =
  if not handle.closed:
    handle.closed = true

iterator queryIter*[K, V](
    handle: var FsQueryHandle[K, V]
): ?!DbQueryResponse[K, V] =
  let root = $(handle.env.self.root)
  let basePath = $(handle.env.basePath)

  for path in basePath.dirIter():
    if handle.cancel:
      break

    var
      basePath = $handle.env.basePath
      keyPath = basePath

    keyPath.removePrefix(root)
    keyPath = keyPath / path.changeFileExt("")
    keyPath = keyPath.replace("\\", "/")

    let
      flres = (basePath / path).absolutePath().catch
    if flres.isErr():
      yield DbQueryResponse[K,V].failure flres.error()
      continue

    let
      key = K.toKey($Key.init(keyPath).expect("valid key"))
      data =
        if handle.query.value:
          let res = readFile[V](handle.env.self, flres.get)
          if res.isErr():
            yield DbQueryResponse[K,V].failure res.error()
            continue
          res.get()
        else:
          V.new()

    yield success (key.some, data)
  handle.close()

proc newFSBackend*[K,V](root: string,
                          depth = 2,
                          caseSensitive = true,
                          ignoreProtected = false
                         ): ?!FSBackend[K,V] =

  let root = ? (
    block:
      if root.isAbsolute: root
      else: getCurrentDir() / root).catch

  if not dirExists(root):
    return failure "directory does not exist: " & root

  success FSBackend[K,V](
    root: DataBuffer.new root,
    ignoreProtected: ignoreProtected,
    depth: depth)
