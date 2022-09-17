import std/algorithm
import std/hashes
import std/oids
import std/sequtils
import std/strutils

import pkg/questionable
import pkg/questionable/results
from pkg/stew/results as stewResults import get, isErr
import pkg/upraises

export hashes

push: {.upraises: [].}

type
  Namespace* = object
    field*: string
    value*: string

  Key* = object
    namespaces*: seq[Namespace]

const
  delimiter = ":"
  separator = "/"

# TODO: operator/s for combining string|Namespace,string|Namespace
# TODO: lifting from ?![Namespace|Key] for various ops

func init*(
  T: type Namespace,
  field, value: string): ?!T =

  if value.strip == "":
    return failure "value string must not be all whitespace or empty"

  if value.contains(delimiter):
    return failure "value string must not contain delimiter \"" &
      delimiter & "\""

  if value.contains(separator):
    return failure "value string must not contain separator \"" &
      separator & "\""

  if field != "":
    if field.strip == "":
      return failure "field string must not be all whitespace"

    if field.contains(delimiter):
      return failure "field string must not contain delimiter \"" &
        delimiter & "\""

    if field.contains(separator):
      return failure "field string must not contain separator \"" &
        separator & "\""

  success T(field: field, value: value)

func init*(T: type Namespace, id: string): ?!T =
  if id.strip == "":
    return failure "id string must not be all whitespace or empty"

  if id.contains(separator):
    return failure "id string must not contain separator \"" & separator & "\""

  if id == delimiter:
    return failure "value in id string \"[field]" & delimiter &
      "[value]\" must not be empty"

  if id.count(delimiter) > 1:
    return failure "id string must not contain more than one delimiter \"" &
      delimiter & "\""

  let
    (field, value) = block:
      let parts = id.split(delimiter)
      if parts.len > 1:
        (parts[0], parts[^1])
      else:
        ("", parts[^1])

  T.init(field, value)

func id*(self: Namespace): string =
  if self.field.len > 0:
    self.field & delimiter & self.value
  else:
    self.value

func hash*(namespace: Namespace): Hash =
  hash(namespace.id)

func `$`*(namespace: Namespace): string =
  "Namespace(" & namespace.id & ")"

func init*(T: type Key, namespaces: varargs[Namespace]): ?!T =
  if namespaces.len == 0:
    failure "namespaces must contain at least one Namespace"
  else:
    success T(namespaces: @namespaces)

func init*(T: type Key, namespaces: varargs[string]): ?!T =
  if namespaces.len == 0:
    failure "namespaces must contain at least one Namespace id string"
  else:
    success T(
      namespaces: namespaces.mapIt(
        ?Namespace.init(it)
    ))

func init*(T: type Key, id: string): ?!T =
  if id == "":
    return failure "id string must contain at least one Namespace"

  if id.strip == "":
    return failure "id string must not be all whitespace"

  let
    nsStrs = id.split(separator).filterIt(it != "")

  if nsStrs.len == 0:
    return failure "id string must not contain only one or more separator " &
      "\"" & separator & "\""

  Key.init(nsStrs)

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

func reversed*(self: Key): Key =
  Key(namespaces: self.namespaces.reversed)

func reverse*(self: Key): Key =
  self.reversed

func name*(self: Key): string =
  if self.len > 0:
    return self[^1].value

func `type`*(self: Key): string =
  if self.len > 0:
    return self[^1].field

func id*(self: Key): string =
  separator & self.namespaces.mapIt(it.id).join(separator)

func root*(self: Key): bool =
  self.len == 1

func parent*(self: Key): ?!Key =
  if self.root:
    failure "key has no parent"
  else:
    success Key(namespaces: self.namespaces[0..^2])

func path*(self: Key): ?!Key =
  let
    parent = ?self.parent

  if self[^1].field == "":
    return success parent

  let ns = parent.namespaces & @[Namespace(value: self[^1].field)]
  success Key(namespaces: ns)

func child*(self: Key, ns: Namespace): Key =
  Key(namespaces: self.namespaces & @[ns])

func `/`*(self: Key, ns: Namespace): Key =
  self.child(ns)

func child*(self: Key, namespaces: varargs[Namespace]): Key =
  Key(namespaces: self.namespaces & @namespaces)

func child*(self, key: Key): Key =
  Key(namespaces: self.namespaces & key.namespaces)

func `/`*(self, key: Key): Key =
  self.child(key)

func child*(self: Key, keys: varargs[Key]): Key =
  Key(namespaces: self.namespaces & concat(keys.mapIt(it.namespaces)))

func child*(self: Key, ids: varargs[string]): ?!Key =
  success self.child(ids.filterIt(it != "").mapIt( ?Key.init(it) ))

func relative*(self: Key, parent: Key): ?!Key =
  ## Get a key relative to parent from current key
  ##

  if self.len < parent.len:
    return failure "Not a parent of this key!"

  Key.init(self.namespaces[parent.namespaces.high..self.namespaces.high])

func `/`*(self: Key, id: string): ?!Key =
  self.child(id)

func ancestor*(self, other: Key): bool =
  if other.len <= self.len: false
  else: other.namespaces[0..<self.len] == self.namespaces

func descendant*(self, other: Key): bool =
  other.ancestor(self)

func hash*(key: Key): Hash {.inline.} =
  hash(key.id)

func `$`*(key: Key): string =
  "Key(" & key.id & ")"
