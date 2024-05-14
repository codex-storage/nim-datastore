import std/algorithm
import std/hashes
import std/oids
import std/sequtils
import std/strutils
import std/strformat

import pkg/questionable
import pkg/questionable/results

import ./namespace

export hashes, namespace

type
  Key* = object
    namespaces*: seq[Namespace]

func init*(T: type Key, namespaces: varargs[Namespace]): ?!T =
  success T(namespaces: @namespaces)

func init*(T: type Key, namespaces: varargs[string]): ?!T =
  var self = T()
  for s in namespaces:
    self.namespaces &= s
      .split( Separator )
      .filterIt( it.len > 0 )
      .mapIt( ?Namespace.init(it) )

  return success self

func init*(T: type Key, keys: varargs[Key]): ?!T =
  success T(
    namespaces: keys
    .mapIt(it.namespaces)
    .concat)

func list*(self: Key): seq[Namespace] =
  self.namespaces

proc random*(T: type Key): string =
  $genOid()

template `[]`*(key: Key, x: auto): auto =
  key.namespaces[x]

func len*(self: Key): int =
  self.namespaces.len

iterator items*(key: Key): Namespace =
  for k in key.namespaces:
    yield k

func reverse*(self: Key): Key =
  Key(namespaces: self.namespaces.reversed)

func value*(self: Key): string =
  if self.len > 0:
    return self[^1].value

func field*(self: Key): string =
  if self.len > 0:
    return self[^1].field

func id*(self: Key): string =
  Separator & self.namespaces.mapIt(it.id).join(Separator)

func root*(self: Key): bool =
  self.len == 1

func parent*(self: Key): ?!Key =
  if self.root:
    failure "key has no parent"
  else:
    success Key(namespaces: self.namespaces[0..^2])

func path*(self: Key): ?!Key =
  let
    tail =
      if self[^1].field.len > 0:
        self[^1].field
      else:
        self[^1].value

  if self.root:
    return Key.init(tail)

  return success Key(
    namespaces: (?self.parent).namespaces &
    @[Namespace(value: tail)])

func child*(self: Key, namespaces: varargs[Namespace]): Key =
  Key(namespaces: self.namespaces & @namespaces)

func `/`*(self: Key, ns: Namespace): Key =
  self.child(ns)

func child*(self: Key, keys: varargs[Key]): Key =
  Key(namespaces: self.namespaces & concat(keys.mapIt(it.namespaces)))

func `/`*(self, key: Key): Key =
  self.child(key)

func child*(self: Key, ids: varargs[string]): ?!Key =
  success self.child(ids.filterIt(it != "").mapIt( ?Key.init(it) ))

func `/`*(self: Key, id: string): ?!Key =
  self.child(id)

func relative*(self: Key, parent: Key): ?!Key =
  ## Get a key relative to parent from current key
  ##

  if self.len < parent.len:
    return failure "Not a parent of this key!"

  Key.init(self.namespaces[parent.namespaces.high..self.namespaces.high])

func ancestor*(self, other: Key): bool =
  if other.len <= self.len: false
  else: other.namespaces[0..<self.len] == self.namespaces

func descendant*(self, other: Key): bool =
  other.ancestor(self)

func hash*(key: Key): Hash {.inline.} =
  hash(key.id)

func `$`*(key: Key): string =
  key.id
