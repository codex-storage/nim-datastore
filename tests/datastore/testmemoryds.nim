import std/options
import std/sequtils
import std/os
from std/algorithm import sort, reversed

import pkg/asynctest
import pkg/chronos
import pkg/stew/results
import pkg/stew/byteutils

import pkg/datastore/memoryds

import ./dscommontests
import ./querycommontests

suite "Test Basic MemoryDatastore":
  let
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  var
    memStore: MemoryDatastore

  setupAll:
    memStore = MemoryDatastore.new()

  basicStoreTests(memStore, key, bytes, otherBytes)

suite "Test Query":

  var ds: MemoryDatastore

  setup:
    ds = MemoryDatastore.new()

  queryTests(ds, false)
