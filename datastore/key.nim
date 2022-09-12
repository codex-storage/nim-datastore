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

proc init*(
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

proc init*(T: type Namespace, id: string): ?!T =
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

proc id*(self: Namespace): string =
  if self.field.len > 0:
    self.field & delimiter & self.value
  else:
    self.value

proc `$`*(namespace: Namespace): string =
  "Namespace(" & namespace.id & ")"

proc init*(T: type Key, namespaces: varargs[Namespace]): ?!T =
  if namespaces.len == 0:
    failure "namespaces must contain at least one Namespace"
  else:
    success T(namespaces: @namespaces)

proc init*(T: type Key, namespaces: varargs[string]): ?!T =
  if namespaces.len == 0:
    failure "namespaces must contain at least one Namespace id string"
  else:
    success T(
      namespaces: namespaces.mapIt(
        ?Namespace.init(it)
    ))

proc init*(T: type Key, id: string): ?!T =
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

proc list*(self: Key): seq[Namespace] =
  self.namespaces

proc random*(T: type Key): string =
  $genOid()

template `[]`*(key: Key, x: auto): auto =
  key.namespaces[x]

proc len*(self: Key): int =
  self.namespaces.len

iterator items*(key: Key): Namespace =
  for k in key.namespaces:
    yield k

proc reversed*(self: Key): Key =
  Key(namespaces: self.namespaces.reversed)

proc reverse*(self: Key): Key =
  self.reversed

proc name*(self: Key): string =
  if self.len > 0:
    return self[^1].value

proc `type`*(self: Key): string =
  if self.len > 0:
    return self[^1].field

proc id*(self: Key): string =
  separator & self.namespaces.mapIt(it.id).join(separator)

proc isTopLevel*(self: Key): bool =
  self.len == 1

proc parent*(self: Key): ?!Key =
  if self.isTopLevel:
    failure "key has no parent"
  else:
    success Key(namespaces: self.namespaces[0..^2])

proc path*(self: Key): ?!Key =
  let
    parent = ? self.parent

  if self[^1].field == "":
    return success parent

  success Key(namespaces: parent.namespaces & @[Namespace(value: self[^1].field)])

proc child*(self: Key, ns: Namespace): Key =
  Key(namespaces: self.namespaces & @[ns])

proc `/`*(self: Key, ns: Namespace): Key =
  self.child(ns)

proc child*(self: Key, namespaces: varargs[Namespace]): Key =
  Key(namespaces: self.namespaces & @namespaces)

proc child*(self, key: Key): Key =
  Key(namespaces: self.namespaces & key.namespaces)

proc `/`*(self, key: Key): Key =
  self.child(key)

proc child*(self: Key, keys: varargs[Key]): Key =
  Key(namespaces: self.namespaces & concat(keys.mapIt(it.namespaces)))

proc child*(self: Key, ids: varargs[string]): ?!Key =
  success self.child(ids.filterIt(it != "").mapIt( ?Key.init(it) ))

proc `/`*(self: Key, id: string): ?!Key =
  self.child(id)

proc isAncestorOf*(self, other: Key): bool =
  if other.len <= self.len: false
  else: other.namespaces[0..<self.len] == self.namespaces

proc isDescendantOf*(self, other: Key): bool =
  other.isAncestorOf(self)

proc `$`*(key: Key): string =
  "Key(" & key.id & ")"
