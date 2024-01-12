
import std/hashes

import pkg/questionable
import pkg/questionable/results
import pkg/upraises

push: {.upraises: [].}

const
  Delimiter* = ":"
  Separator* = "/"

type
  Namespace* = object
    field*: string
    value*: string

func init*(T: type Namespace, field, value: string): ?!T =
  if value.contains(Delimiter):
      return failure ("value string must not contain Delimiter " & Delimiter)

  if value.contains(Separator):
    return failure ("value string must not contain Separator " & Separator)

  if field.contains(Delimiter):
    return failure ("field string must not contain Delimiter " & Delimiter)

  if field.contains(Separator):
    return failure ("field string must not contain Separator " & Separator)

  success T(field: field, value: value)

func init*(T: type Namespace, id: string): ?!T =
  if id.len > 0:
    if id.contains(Separator):
      return failure (&"id string must not contain Separator " & Separator)

    if id.count(Delimiter) > 1:
      return failure (&"id string must not contain more than one " & Delimiter)

  let
    (field, value) = block:
      let parts = id.split(Delimiter)
      if parts.len > 1:
        (parts[0], parts[^1])
      else:
        ("", parts[^1])

  T.init(field.strip, value.strip)

func id*(self: Namespace): string =
  if self.field.len > 0:
    self.field & Delimiter & self.value
  else:
    self.value

func hash*(namespace: Namespace): Hash =
  hash(namespace.id)

func `$`*(namespace: Namespace): string =
  namespace.id
