import std/options
import std/sequtils
import std/os
from std/algorithm import sort, reversed

import pkg/unittest2
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore/fsds
import pkg/datastore/key
import pkg/datastore/backend

import ./backendCommonTests

suite "Test Basic FSDatastore":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    keyFull = Key.init("/a/b").tryGet()
    key = KeyId.new keyFull.id()
    bytes = DataBuffer.new "some bytes"
    otherBytes = DataBuffer.new "some other bytes".toBytes

  var batch: seq[tuple[key: KeyId, data: DataBuffer]]
  for k in 0..<100:
    let kk = Key.init($keyFull, $k).tryGet().id()
    batch.add( (KeyId.new kk, DataBuffer.new @[k.byte]) )

  removeDir(basePathAbs)
  require(not dirExists(basePathAbs))
  createDir(basePathAbs)

  var
    fsStore = newFSDatastore[KeyId, DataBuffer](root = basePathAbs, depth = 3).tryGet()

  testBasicBackend(fsStore, key, bytes, otherBytes, batch)

  removeDir(basePathAbs)
  require(not dirExists(basePathAbs))

suite "Test Basic FSDatastore":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  var batch: seq[tuple[key: string, data: seq[byte]]]
  for k in 0..<100:
    let kk = Key.init($key, $k).tryGet().id()
    batch.add( (kk, @[k.byte]) )

  removeDir(basePathAbs)
  require(not dirExists(basePathAbs))
  createDir(basePathAbs)

  var
    fsStore = newFSDatastore[Key, seq[byte]](root = basePathAbs, depth = 3).tryGet()

  testBasicBackend(fsStore, key, bytes, otherBytes, batch)

  removeDir(basePathAbs)
  require(not dirExists(basePathAbs))

# suite "Test Misc FSDatastore":
#   let
#     path = currentSourcePath() # get this file's name
#     basePath = "tests_data"
#     basePathAbs = path.parentDir / basePath
#     bytes = "some bytes".toBytes

#   setup:
#     removeDir(basePathAbs)
#     require(not dirExists(basePathAbs))
#     createDir(basePathAbs)

#   teardown:
#     removeDir(basePathAbs)
#     require(not dirExists(basePathAbs))

#   test "Test validDepth()":
#     let
#       fs = FSDatastore.new(root = "/", depth = 3).tryGet()
#       invalid = Key.init("/a/b/c/d").tryGet()
#       valid = Key.init("/a/b/c").tryGet()

#     check:
#       not fs.validDepth(invalid)
#       fs.validDepth(valid)

#   test "Test invalid key (path) depth":
#     let
#       fs = FSDatastore.new(root = basePathAbs, depth = 3).tryGet()
#       key = Key.init("/a/b/c/d").tryGet()

#     check:
#       (await fs.put(key, bytes)).isErr
#       (await fs.get(key)).isErr
#       (await fs.delete(key)).isErr
#       (await fs.has(key)).isErr

#   test "Test valid key (path) depth":
#     let
#       fs = FSDatastore.new(root = basePathAbs, depth = 3).tryGet()
#       key = Key.init("/a/b/c").tryGet()

#     check:
#       (await fs.put(key, bytes)).isOk
#       (await fs.get(key)).isOk
#       (await fs.delete(key)).isOk
#       (await fs.has(key)).isOk

#   test "Test key cannot write outside of root":
#     let
#       fs = FSDatastore.new(root = basePathAbs, depth = 3).tryGet()
#       key = Key.init("/a/../../c").tryGet()

#     check:
#       (await fs.put(key, bytes)).isErr
#       (await fs.get(key)).isErr
#       (await fs.delete(key)).isErr
#       (await fs.has(key)).isErr

#   test "Test key cannot convert to invalid path":
#     let
#       fs = FSDatastore.new(root = basePathAbs).tryGet()

#     for c in invalidFilenameChars:
#       if c == ':': continue
#       if c == '/': continue

#       let
#         key = Key.init("/" & c).tryGet()

#       check:
#         (await fs.put(key, bytes)).isErr
#         (await fs.get(key)).isErr
#         (await fs.delete(key)).isErr
#         (await fs.has(key)).isErr


# suite "Test Query":
#   let
#     path = currentSourcePath() # get this file's name
#     basePath = "tests_data"
#     basePathAbs = path.parentDir / basePath

#   var
#     ds: FSDatastore

#   setup:
#     removeDir(basePathAbs)
#     require(not dirExists(basePathAbs))
#     createDir(basePathAbs)

#     ds = FSDatastore.new(root = basePathAbs, depth = 5).tryGet()

#   teardown:

#     removeDir(basePathAbs)
#     require(not dirExists(basePathAbs))

#   queryTests(ds, false)
