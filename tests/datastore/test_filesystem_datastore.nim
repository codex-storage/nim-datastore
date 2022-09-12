import std/options
import std/os

import pkg/asynctest/unittest2
import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/stew/byteutils
from pkg/stew/results as stewResults import get, isOk

import ../../datastore/filesystem_datastore
import ./templates

suite "FileSystemDatastore":
  # assumes tests/test_all is run from project root, e.g. with `nimble test`
  let
    root = "tests" / "test_data"
    rootAbs = getCurrentDir() / root

  setup:
    removeDir(rootAbs)
    require(not dirExists(rootAbs))
    createDir(rootAbs)

  teardown:
    removeDir(rootAbs)
    require(not dirExists(rootAbs))

  asyncTest "new":
    var
      dsRes: ?!FileSystemDatastore

    dsRes = FileSystemDatastore.new(rootAbs / "missing")

    check: dsRes.isErr

    dsRes = FileSystemDatastore.new(rootAbs)

    check: dsRes.isOk

    dsRes = FileSystemDatastore.new(root)

    check: dsRes.isOk

  asyncTest "accessors":
    let
      ds = FileSystemDatastore.new(root).get

    check: ds.root == rootAbs

  asyncTest "helpers":
      let
        ds = FileSystemDatastore.new(root).get

      check:
        # see comment in ../../datastore/filesystem_datastore re: whether path
        # equivalence of e.g. Key(/a:b) and Key(/a/b) is problematic
        ds.path(Key.init("a").get) == rootAbs / "a" & objExt
        ds.path(Key.init("a:b").get) == rootAbs / "a" / "b" & objExt
        ds.path(Key.init("a/b").get) == rootAbs / "a" / "b" & objExt
        ds.path(Key.init("a:b/c").get) == rootAbs / "a" / "b" / "c" & objExt
        ds.path(Key.init("a/b/c").get) == rootAbs / "a" / "b" / "c" & objExt
        ds.path(Key.init("a:b/c:d").get) == rootAbs / "a" / "b" / "c" / "d" & objExt
        ds.path(Key.init("a/b/c:d").get) == rootAbs / "a" / "b" / "c" / "d" & objExt
        ds.path(Key.init("a/b/c/d").get) == rootAbs / "a" / "b" / "c" / "d" & objExt

  asyncTest "put":
    let
      ds = FileSystemDatastore.new(root).get
      key = Key.init("a:b/c/d:e").get
      path = ds.path(key)

    var
      bytes: seq[byte]
      putRes = await ds.put(key, bytes)

    check:
      putRes.isOk
      readFile(path).toBytes == bytes

    bytes = @[1.byte, 2.byte, 3.byte]

    putRes = await ds.put(key, bytes)

    check:
      putRes.isOk
      readFile(path).toBytes == bytes

    bytes = @[4.byte, 5.byte, 6.byte]

    putRes = await ds.put(key, bytes)

    check:
      putRes.isOk
      readFile(path).toBytes == bytes

  asyncTest "delete":
    let
      bytes = @[1.byte, 2.byte, 3.byte]
      ds = FileSystemDatastore.new(root).get

    var
      key = Key.init("a:b/c/d:e").get
      path = ds.path(key)

    let
      putRes = await ds.put(key, bytes)

    assert putRes.isOk

    var
      delRes = await ds.delete(key)

    check:
      delRes.isOk
      not fileExists(path)
      dirExists(parentDir(path))

    key = Key.init("X/Y/Z").get
    path = ds.path(key)
    assert not fileExists(path)

    delRes = await ds.delete(key)

    check: delRes.isOk

  asyncTest "contains":
    let
      bytes = @[1.byte, 2.byte, 3.byte]
      ds = FileSystemDatastore.new(root).get

    var
      key = Key.init("a:b/c/d:e").get
      path = ds.path(key)
      putRes = await ds.put(key, bytes)

    assert putRes.isOk

    var
      containsRes = await ds.contains(key)

    check:
      containsRes.isOk
      containsRes.get == true

    key = Key.init("X/Y/Z").get
    path = ds.path(key)
    assert not fileExists(path)

    containsRes = await ds.contains(key)

    check:
      containsRes.isOk
      containsRes.get == false

  asyncTest "get":
    let
      ds = FileSystemDatastore.new(root).get

    var
      bytes: seq[byte]
      key = Key.init("a:b/c/d:e").get
      path = ds.path(key)
      putRes = await ds.put(key, bytes)

    assert putRes.isOk

    var
      getRes = await ds.get(key)
      getOpt = getRes.get

    check: getOpt.isSome and getOpt.get == bytes

    bytes = @[1.byte, 2.byte, 3.byte]
    putRes = await ds.put(key, bytes)

    assert putRes.isOk

    getRes = await ds.get(key)
    getOpt = getRes.get

    check: getOpt.isSome and getOpt.get == bytes

    key = Key.init("X/Y/Z").get
    path = ds.path(key)

    assert not fileExists(path)

    getRes = await ds.get(key)
    getOpt = getRes.get

    check: getOpt.isNone
