
import std/hashes
import std/strformat

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

  if value.strip.len <= 0:
    return failure "value string must not be all whitespace or empty"

  if value.contains(Delimiter):
      return failure (&"value string must not contain Delimiter '{Delimiter}'")
        .catch.expect("should not fail")

  if value.contains(Separator):
    return failure (&"value string must not contain Separator {Separator}")
      .catch.expect("should not fail")

  if field.len > 0:
    if field.strip.len <= 0:
      return failure "field string must not be all whitespace"

    if field.contains(Delimiter):
      return failure (&"field string must not contain Delimiter {Delimiter}")
        .catch.expect("should not fail")

    if field.contains(Separator):
      return failure (&"field string must not contain Separator {Separator}")
        .catch.expect("should not fail")

  success T(field: field, value: value)

func init*(T: type Namespace, id: string): ?!T =
  if id.strip == "":
    return failure "id string must not be all whitespace or empty"

  if id.contains(Separator):
    return failure (&"id string must not contain Separator {Separator}")
      .catch.expect("should not fail")

  if id == Delimiter:
    return failure "value in id string must not be empty"

  if id.count(Delimiter) > 1:
    return failure (&"id string must not contain more than one {Delimiter}")
      .catch.expect("should not fail")

  let
    (field, value) = block:
      let parts = id.split(Delimiter)
      if parts.len > 1:
        (parts[0], parts[^1])
      else:
        ("", parts[^1])

  T.init(field, value)

func id*(self: Namespace): string =
  if self.field.len > 0:
    self.field & Delimiter & self.value
  else:
    self.value

func hash*(namespace: Namespace): Hash =
  hash(namespace.id)

func `$`*(namespace: Namespace): string =
  "Namespace(" & namespace.id & ")"
