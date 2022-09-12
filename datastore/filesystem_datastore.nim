import std/os

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
from pkg/stew/results as stewResults import get, isErr
import pkg/upraises

import ./datastore

export datastore

push: {.upraises: [].}

type
  FileSystemDatastore* = ref object of Datastore
    root: string

const
  objExt* = ".dsobject"

proc new*(
  T: type FileSystemDatastore,
  root: string): ?!T =

  try:
    let
      root = if root.isAbsolute: root
             else: getCurrentDir() / root

    if not dirExists(root):
      failure "directory does not exist: " & root
    else:
      success T(root: root)

  except OSError as e:
    failure e

proc root*(self: FileSystemDatastore): string =
  self.root

proc path*(
  self: FileSystemDatastore,
  key: Key): string =

  var
    segments: seq[string]

  for ns in key:
    without field =? ns.field:
      segments.add ns.value
      continue

    segments.add(field / ns.value)

  # is it problematic that per this logic Key(/a:b) evaluates to the same path
  # as Key(/a/b)? may need to check if/how other Datastore implementations
  # distinguish them

  self.root / joinPath(segments) & objExt

method close*(self: FileSystemDatastore) {.async, locks: "unknown".} =
  discard

method contains*(
  self: FileSystemDatastore,
  key: Key): Future[?!bool] {.async, locks: "unknown".} =

  return success fileExists(self.path(key))

method delete*(
  self: FileSystemDatastore,
  key: Key): Future[?!void] {.async, locks: "unknown".} =

  let
    path = self.path(key)

  try:
    removeFile(path)
    return success()

    # removing an empty directory might lead to surprising behavior depending
    # on what the user specified as the `root` of the FileSystemDatastore, so
    # until further consideration, empty directories will be left in place

  except OSError as e:
    return failure e

method get*(
  self: FileSystemDatastore,
  key: Key): Future[?!(?seq[byte])] {.async, locks: "unknown".} =

  # to support finer control of memory allocation, maybe could/should change
  # the signature of `get` so that it has a 3rd parameter
  # `bytes: var openArray[byte]` and return type `?!bool`; this variant with
  # return type `?!(?seq[byte])` would be a special case (convenience method)
  # calling the former after allocating a seq with size automatically
  # determined via `getFileSize`

  let
    path = self.path(key)
    containsRes = await self.contains(key)

  if containsRes.isErr: return failure containsRes.error.msg

  if containsRes.get:
    var
      file: File

    if not file.open(path):
      return failure "unable to open file: " & path
    else:
      try:
        let
          size = file.getFileSize

        var
          bytes: seq[byte]

        if size > 0:
          newSeq(bytes, size)

          let
            bytesRead = file.readBytes(bytes, 0, size)

          if bytesRead < size:
            return failure $bytesRead & " bytes were read from " & path &
              " but " & $size & " bytes were expected"

        return success bytes.some

      except IOError as e:
        return failure e

      finally:
        file.close

  else:
    return success seq[byte].none

method put*(
  self: FileSystemDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async, locks: "unknown".} =

  let
    path = self.path(key)

  try:
    createDir(parentDir(path))
    if data.len > 0: writeFile(path, data)
    else: writeFile(path, "")
    return success()

  except IOError as e:
    return failure e

  except OSError as e:
    return failure e
