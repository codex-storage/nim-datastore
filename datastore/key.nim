import std/algorithm
import std/hashes
import std/oids
import std/sequtils
import std/strutils

import pkg/questionable
import pkg/questionable/results
import pkg/upraises

export hashes

push: {.upraises: [].}

type
  Namespace* = object
    field: ?string
    value: string

  Key* = object
    namespaces: seq[Namespace]

const
  delimiter = ":"
  separator = "/"

# TODO: operator/s for combining string|Namespace,string|Namespace
# TODO: lifting from ?![Namespace|Key] for various ops

proc init*(
  T: type Namespace,
  field, value: string): ?!T =

  if value == "":
    return failure "value string must not be empty"

  if value.strip == "":
    return failure "value string must not be all whitespace"

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

    success T(field: field.some, value: value)
  else:
    success T(field: string.none, value: value)

proc init*(
  T: type Namespace,
  id: string): ?!T =

  if id == "":
    return failure "id string must not be empty"

  if id.strip == "":
    return failure "id string must not be all whitespace"

  if id.contains(separator):
    return failure "id string must not contain separator \"" & separator & "\""

  if id == delimiter:
    return failure "value in id string \"[field]" & delimiter &
      "[value]\" must not be empty"

  let
    s = id.split(delimiter)

  if s.len > 2:
    return failure "id string must not contain more than one delimiter \"" &
      delimiter & "\""

  var
    field: ?string
    value: string

  if s.len == 1:
    value = s[0]
  else:
    value = s[1]

    if value == "":
      return failure "value in id string \"[field]" & delimiter &
        "[value]\" must not be empty"

    if value.strip == "":
      return failure "value in id string \"[field]" & delimiter &
        "[value]\" must not be all whitespace"

    else:
      let
        f = s[0]

      if f != "":
        if f.strip == "":
          return failure "field in id string \"[field]" & delimiter &
            "[value]\" must not be all whitespace"

        else:
          field = f.some

  success T(field: field, value: value)

proc value*(self: Namespace): string =
  self.value

proc field*(self: Namespace): ?string =
  self.field

proc `type`*(self: Namespace): ?string =
  self.field

proc kind*(self: Namespace): ?string =
  self.`type`

proc id*(self: Namespace): string =
  if field =? self.field: field & delimiter & self.value
  else: self.value

proc `$`*(namespace: Namespace): string =
  "Namespace(" & namespace.id & ")"

proc init*(
  T: type Key,
  namespaces: varargs[Namespace]): ?!T =

  if namespaces.len == 0:
    failure "namespaces must contain at least one Namespace"
  else:
    success T(namespaces: @namespaces)

proc init*(
  T: type Key,
  namespaces: varargs[string]): ?!T =

  if namespaces.len == 0:
    failure "namespaces must contain at least one Namespace id string"
  else:
    var
      nss: seq[Namespace]

    for s in namespaces:
      let
        nsRes = Namespace.init(s)
      # if `without ns =? Namespace.init(s), e:` is used `e` is nil in the body
      # at runtime, why?
      without ns =? nsRes:
        return failure "namespaces contains an invalid Namespace: " &
          nsRes.error.msg

      nss.add ns

    success T(namespaces: nss)

proc init*(
  T: type Key,
  id: string): ?!T =

  if id == "":
    return failure "id string must contain at least one Namespace"

  if id.strip == "":
    return failure "id string must not be all whitespace"

  let
    nsStrs = id.split(separator).filterIt(it != "")

  if nsStrs.len == 0:
    return failure "id string must not contain only one or more separator " &
      "\"" & separator & "\""

  let
    keyRes = Key.init(nsStrs)
  # if `without key =? Key.init(nsStrs), e:` is used `e` is nil in the body
  # at runtime, why?
  without key =? keyRes:
    return failure "id string contains an invalid Namespace:" &
      keyRes.error.msg.split(":")[1..^1].join("").replace("\"\"", "\":\"")

  success key

proc namespaces*(self: Key): seq[Namespace] =
  self.namespaces

proc list*(self: Key): seq[Namespace] =
  self.namespaces

proc random*(T: type Key): string =
  $genOid()

template `[]`*(
  key: Key,
  x: auto): auto =

  key.namespaces[x]

proc len*(self: Key): int =
  self.namespaces.len

iterator items*(key: Key): Namespace {.inline.} =
  var
    i = 0

  while i < key.len:
    yield key[i]
    inc i

proc reversed*(self: Key): Key =
  Key(namespaces: self.namespaces.reversed)

proc reverse*(self: Key): Key =
  self.reversed

proc name*(self: Key): string =
  self[^1].value

proc `type`*(self: Key): ?string =
  self[^1].field

proc kind*(self: Key): ?string =
  self.`type`

proc instance*(
  self: Key,
  value: Namespace): Key =

  let
    last = self[^1]

    inst =
      if last.field.isSome:
        @[Namespace(field: last.field, value: value.value)]
      else:
        @[Namespace(field: last.value.some, value: value.value)]

    namespaces =
      if self.namespaces.len == 1:
        inst
      else:
        self.namespaces[0..^2] & inst

  Key(namespaces: namespaces)

proc instance*(self, value: Key): Key =
  self.instance(value[^1])

proc instance*(self, value: Namespace): Key =
  Key(namespaces: @[self]).instance(value)

proc instance*(
  self: Namespace,
  value: Key): Key =

  self.instance(value[^1])

proc instance*(
  self: Key,
  id: string): ?!Key =

  without key =? Key.init(id), e:
    return failure e

  success self.instance(key)

proc isTopLevel*(self: Key): bool =
  self.len == 1

proc parent*(self: Key): ?!Key =
  if self.isTopLevel:
    failure "key has no parent"
  else:
    success Key(namespaces: self.namespaces[0..^2])

proc parent*(self: ?!Key): ?!Key =
  let
    key = ? self

  key.parent

proc path*(self: Key): ?!Key =
  let
    parent = ? self.parent

  without kind =? self[^1].kind:
    return success parent

  success Key(namespaces: parent.namespaces & @[Namespace(value: kind)])

proc path*(self: ?!Key): ?!Key =
  let
    key = ? self

  key.path

proc child*(
  self: Key,
  ns: Namespace): Key =

  Key(namespaces: self.namespaces & @[ns])

proc `/`*(
  self: Key,
  ns: Namespace): Key =

  self.child(ns)

proc child*(
  self: Key,
  namespaces: varargs[Namespace]): Key =

  Key(namespaces: self.namespaces & @namespaces)

proc child*(self, key: Key): Key =
  Key(namespaces: self.namespaces & key.namespaces)

proc `/`*(self, key: Key): Key =
  self.child(key)

proc child*(
  self: Key,
  keys: varargs[Key]): Key =

  Key(namespaces: self.namespaces & concat(keys.mapIt(it.namespaces)))

proc child*(
  self: Key,
  ids: varargs[string]): ?!Key =

  let
    ids = ids.filterIt(it != "")

  var
    keys: seq[Key]

  for id in ids:
    let
      key = ? Key.init(id)

    keys.add key

  success self.child(keys)

proc `/`*(
  self: Key,
  id: string): ?!Key =

  self.child(id)

proc isAncestorOf*(self, other: Key): bool =
  if other.len <= self.len: false
  else: other.namespaces[0..<self.len] == self.namespaces

proc isDescendantOf*(self, other: Key): bool =
  other.isAncestorOf(self)

proc id*(self: Key): string =
  separator & self.namespaces.mapIt(it.id).join(separator)

proc `$`*(key: Key): string =
  "Key(" & key.id & ")"
