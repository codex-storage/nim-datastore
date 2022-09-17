import std/os
import std/sequtils
import std/options

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
from pkg/stew/results as stewResults import get, isErr
import pkg/upraises

import ./datastore

export datastore

push: {.upraises: [].}

const
  # TODO: Add more dirs from relevant OSs

  # Paths should be matched exactly, i.e.
  # we're forbidding this dirs from being
  # touched directly, but subdirectories
  # can still be touched
  ProtectedPaths = [
    "/",
    "/usr",
    "/etc",
    "/home",
    "/Users"]

type
  FSDatastore* = ref object of Datastore
    root*: string
    ignoreProtected: bool

func checkProtected(dir: string): bool =
  dir in ProtectedPaths

proc path*(self: FSDatastore, key: Key): string =
  var
    segments: seq[string]

  for ns in key:
    if ns.field == "":
      segments.add ns.value
      continue

    segments.add(ns.field / ns.value)

  # is it problematic that per this logic Key(/a:b) evaluates to the same path
  # as Key(/a/b)? may need to check if/how other Datastore implementations
  # distinguish them

  self.root / joinPath(segments)

method contains*(self: FSDatastore, key: Key): Future[?!bool] {.async.} =
  return success fileExists(self.path(key))

method delete*(self: FSDatastore, key: Key): Future[?!void] {.async.} =

  let
    path = self.path(key)

  if checkProtected(path):
    return failure "Path is protected!"

  try:
    removeFile(path)
    return success()

    # removing an empty directory might lead to surprising behavior depending
    # on what the user specified as the `root` of the FSDatastore, so
    # until further consideration, empty directories will be left in place

  except OSError as e:
    return failure e

method get*(self: FSDatastore, key: Key): Future[?!seq[byte]] {.async.} =

  # to support finer control of memory allocation, maybe could/should change
  # the signature of `get` so that it has a 3rd parameter
  # `bytes: var openArray[byte]` and return type `?!bool`; this variant with
  # return type `?!(?seq[byte])` would be a special case (convenience method)
  # calling the former after allocating a seq with size automatically
  # determined via `getFileSize`

  let
    path = self.path(key)

  if checkProtected(path):
    return failure "Path is protected!"

  if not fileExists(path):
    return success(newSeq[byte]())

  var
    file: File

  defer:
    file.close

  if not file.open(path):
    return failure "unable to open file: " & path

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

  except IOError as e:
    return failure e

method put*(
  self: FSDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async, locks: "unknown".} =

  let
    path = self.path(key)

  if checkProtected(path):
    return failure "Path is protected!"

  try:
    createDir(parentDir(path))
    writeFile(path, data)
  except IOError as e:
    return failure e
  except OSError as e:
    return failure e

  return success()

# method query*(
#   self: FSDatastore,
#   query: ...): Future[?!(?...)] {.async, locks: "unknown".} =
#
#   return success ....some

proc new*(
  T: type FSDatastore,
  root: string,
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
    ignoreProtected: ignoreProtected)
