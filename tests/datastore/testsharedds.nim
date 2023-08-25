import std/options
import std/sequtils
import std/os
from std/algorithm import sort, reversed

import pkg/asynctest/unittest2
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore/sharedds

import ./dscommontests
import ./querycommontests

suite "Test Basic SharedDatastore":

  test "check create":

    var sds: SharedDatastore

    let backend = ThreadBackend(
      kind: TestBackend,
    )
    let res = await newSharedDataStore(backend)
    check res.isOk()
    sds = res.get()
    # echo "sds: ", repr sds

    echo "\n\n=== put ==="
    let key1 = Key.init("/a").tryGet
    let res1 = await sds.put(key1, "value for 1".toBytes())
    echo "res1: ", res1.repr

# suite "Test Basic FSDatastore":
#   let
#     path = currentSourcePath() # get this file's name
#     basePath = "tests_data"
#     basePathAbs = path.parentDir / basePath
#     key = Key.init("/a/b").tryGet()
#     bytes = "some bytes".toBytes
#     otherBytes = "some other bytes".toBytes

#   var
#     fsStore: FSDatastore

#   setupAll:
#     removeDir(basePathAbs)
#     require(not dirExists(basePathAbs))
#     createDir(basePathAbs)

#     fsStore = FSDatastore.new(root = basePathAbs, depth = 3).tryGet()

#   teardownAll:
#     removeDir(basePathAbs)
#     require(not dirExists(basePathAbs))

#   basicStoreTests(fsStore, key, bytes, otherBytes)

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
