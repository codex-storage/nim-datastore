import std/options

import pkg/stew/results
import pkg/unittest2

import ../../datastore/key

suite "Namespace":
  test "init":
    var
      nsRes: Result[Namespace, ref CatchableError]

    nsRes = Namespace.init("a", "")

    check: nsRes.isErr

    nsRes = Namespace.init("a", "   ")

    check: nsRes.isErr

    nsRes = Namespace.init("a", ":")

    check: nsRes.isErr

    nsRes = Namespace.init("a", "/")

    check: nsRes.isErr

    nsRes = Namespace.init(":", "b")

    check: nsRes.isErr

    nsRes = Namespace.init("/", "b")

    check: nsRes.isErr

    nsRes = Namespace.init("", "b")

    check: nsRes.isOk

    nsRes = Namespace.init("   ", "b")

    check: nsRes.isErr

    nsRes = Namespace.init("a", "b")

    check: nsRes.isOk

    nsRes = Namespace.init("")

    check: nsRes.isErr

    nsRes = Namespace.init("   ")

    check: nsRes.isErr

    nsRes = Namespace.init("/")

    check: nsRes.isErr

    nsRes = Namespace.init(":")

    check: nsRes.isErr

    nsRes = Namespace.init("a:b:c")

    check: nsRes.isErr

    nsRes = Namespace.init("a")

    check: nsRes.isOk

    nsRes = Namespace.init("a:")

    check: nsRes.isErr

    nsRes = Namespace.init("a:   ")

    check: nsRes.isErr

    nsRes = Namespace.init("   :b")

    check: nsRes.isErr

    nsRes = Namespace.init("a:b")

    check: nsRes.isOk

    nsRes = Namespace.init(":b")

    check: nsRes.isOk

  test "accessors":
    var
      ns: Namespace

    ns = Namespace.init("", "b").get

    check:
      ns.value == "b"
      ns.field.isNone

    ns = Namespace.init("a", "b").get

    check:
      ns.value == "b"
      ns.field.isSome and ns.field.get == "a"

    ns = Namespace.init(":b").get

    check:
      ns.value == "b"
      ns.field.isNone

    ns = Namespace.init("a:b").get

    check:
      ns.value == "b"
      ns.field.isSome and ns.field.get == "a"

    check:
      ns.`type`.get == ns.field.get
      ns.kind.get == ns.field.get

  test "equality":
    check:
      Namespace.init("a").get == Namespace.init("a").get
      Namespace.init("a").get != Namespace.init("b").get
      Namespace.init("", "b").get == Namespace.init("", "b").get
      Namespace.init("", "b").get == Namespace.init("b").get
      Namespace.init(":b").get == Namespace.init("b").get
      Namespace.init("", "b").get != Namespace.init("", "a").get
      Namespace.init("", "b").get != Namespace.init("a").get
      Namespace.init(":b").get != Namespace.init("a").get
      Namespace.init("a", "b").get == Namespace.init("a", "b").get
      Namespace.init("a", "b").get == Namespace.init("a:b").get
      Namespace.init("a:b").get == Namespace.init("a:b").get
      Namespace.init("a", "b").get != Namespace.init("b", "a").get
      Namespace.init("a", "b").get != Namespace.init("b:a").get
      Namespace.init("a:b").get != Namespace.init("b:a").get
      Namespace.init("a").get != Namespace.init("a:b").get

  test "serialization":
    var
      ns: Namespace

    ns = Namespace.init(":b").get

    check:
      ns.id == "b"
      $ns == "Namespace(" & ns.id & ")"

    ns = Namespace.init("a:b").get

    check:
      ns.id == "a:b"
      $ns == "Namespace(" & ns.id & ")"

suite "Key":
  test "init":
    var
      keyRes: Result[Key, ref CatchableError]
      nss: seq[Namespace]

    keyRes = Key.init(nss)

    check: keyRes.isErr

    nss = @[Namespace.init("a").get]

    keyRes = Key.init(nss)

    check: keyRes.isOk

    var
      nsStrs: seq[string]

    keyRes = Key.init(nsStrs)

    check: keyRes.isErr

    nsStrs = @[":"]

    keyRes = Key.init(nsStrs)

    check: keyRes.isErr

    nsStrs = @["/"]

    keyRes = Key.init(nsStrs)

    check: keyRes.isErr

    nsStrs = @["a:b"]

    keyRes = Key.init(nsStrs)

    check: keyRes.isOk

    keyRes = Key.init("")

    check: keyRes.isErr

    keyRes = Key.init("   ")

    check: keyRes.isErr

    keyRes = Key.init("/")

    check: keyRes.isErr

    keyRes = Key.init("///")

    check: keyRes.isErr

    keyRes = Key.init(":")

    check: keyRes.isErr

    keyRes = Key.init("::")

    check: keyRes.isErr

    keyRes = Key.init("a:")

    check: keyRes.isErr

    keyRes = Key.init("a:b/c:")

    check: keyRes.isErr

    keyRes = Key.init(":b")

    check: keyRes.isOk

    keyRes = Key.init("a:b")

    check: keyRes.isOk

    keyRes = Key.init("a:b/c")

    check: keyRes.isOk

    keyRes = Key.init("a:b/:c")

    check: keyRes.isOk

    keyRes = Key.init("/a:b/c/")

    check: keyRes.isOk

    keyRes = Key.init("///a:b///c///")

    check: keyRes.isOk

  test "accessors":
    let
      key = Key.init("/a:b/c/d:e").get

    check:
      key.namespaces == @[
        Namespace.init("a:b").get,
        Namespace.init("c").get,
        Namespace.init("d:e").get
      ]

      key.list == key.namespaces

  test "equality":
    check:
      Key.init(Namespace.init("a:b").get, Namespace.init("c").get).get == Key.init("a:b/c").get
      Key.init("a:b", "c").get == Key.init("a:b/c").get
      Key.init("a:b/c").get == Key.init("a:b/c").get
      Key.init(Namespace.init("a:b").get, Namespace.init("c").get).get != Key.init("c:b/a").get
      Key.init("a:b", "c").get != Key.init("c:b/a").get
      Key.init("a:b/c").get != Key.init("c:b/a").get
      Key.init("a:b/c").get == Key.init("/a:b/c/").get
      Key.init("a:b/c").get == Key.init("///a:b///c///").get
      Key.init("a:b/c").get != Key.init("///a:b///d///").get
      Key.init("a").get != Key.init("a:b").get
      Key.init("a").get != Key.init("a/b").get
      Key.init("a/b/c").get != Key.init("a/b").get
      Key.init("a:X/b/c").get == Key.init("a:X/b/c").get
      Key.init("a/b:X/c").get == Key.init("a/b:X/c").get
      Key.init("a/b/c:X").get == Key.init("a/b/c:X").get
      Key.init("a:X/b/c:X").get == Key.init("a:X/b/c:X").get
      Key.init("a:X/b:X/c").get == Key.init("a:X/b:X/c").get
      Key.init("a/b:X/c:X").get == Key.init("a/b:X/c:X").get
      Key.init("a:X/b:X/c:X").get == Key.init("a:X/b:X/c:X").get
      Key.init("a/b/c").get != Key.init("a:X/b/c").get
      Key.init("a/b/c").get != Key.init("a/b:X/c").get
      Key.init("a/b/c").get != Key.init("a/b/c:X").get
      Key.init("a/b/c").get != Key.init("a:X/b/c:X").get
      Key.init("a/b/c").get != Key.init("a:X/b:X/c").get
      Key.init("a/b/c").get != Key.init("a/b:X/c:X").get
      Key.init("a/b/c").get != Key.init("a:X/b:X/c:X").get

  test "helpers":
    check: Key.random.len == 24

    let
      key = Key.init("/a:b/c/d:e").get

    check:
      key[1] == Namespace.init("c").get
      key[1..^1] == @[Namespace.init("c").get, Namespace.init("d:e").get]
      key[^1] == Namespace.init("d:e").get

    check: key.len == key.namespaces.len

    var
      nss: seq[Namespace]

    for ns in key:
      nss.add ns

    check:
      nss == @[
        Namespace.init("a:b").get,
        Namespace.init("c").get,
        Namespace.init("d:e").get
      ]

    check:
      key.reversed.namespaces == @[
        Namespace.init("d:e").get,
        Namespace.init("c").get,
        Namespace.init("a:b").get
      ]

      key.reverse == key.reversed

    check: key.name == "e"

    check:
      key.`type` == key[^1].`type`
      key.kind == key.`type`

    check:
      key.instance(Namespace.init("f:g").get) == Key.init("a:b/c/d:g").get
      Key.init("a:b").get.instance(Namespace.init(":c").get) == Key.init("a:c").get
      Key.init(":b").get.instance(Namespace.init(":c").get) == Key.init("b:c").get
      Key.init(":b").get.instance(key) == Key.init("b:e").get
      Namespace.init("a:b").get.instance(Namespace.init("c").get) == Key.init("a:c").get
      Namespace.init(":b").get.instance(Namespace.init("c").get) == Key.init("b:c").get
      Namespace.init("a:b").get.instance(key) == Key.init("a:e").get
      Namespace.init(":b").get.instance(key) == Key.init("b:e").get
      Key.init(":b").get.instance("").isErr
      Key.init(":b").get.instance(":").isErr
      Key.init(":b").get.instance("/").isErr
      Key.init(":b").get.instance("//").isErr
      Key.init(":b").get.instance("///").isErr
      Key.init(":b").get.instance("a").get == Key.init("b:a").get
      Key.init(":b").get.instance(":b").get == Key.init("b:b").get
      Key.init(":b").get.instance("a:b").get == Key.init("b:b").get
      Key.init(":b").get.instance("/a:b/c/d:e").get == Key.init("b:e").get
      Key.init("a:b").get.instance("a").get == Key.init("a:a").get
      Key.init("a:b").get.instance(":b").get == Key.init("a:b").get
      Key.init("a:b").get.instance("a:b").get == Key.init("a:b").get
      Key.init("a:b").get.instance("/a:b/c/d:e").get == Key.init("a:e").get

    check:
      Key.init(":b").get.isTopLevel
      not Key.init(":b/c").get.isTopLevel

    check:
      Key.init(":b").get.parent.isErr
      Key.init(":b").parent.isErr
      key.parent.get == Key.init("a:b/c").get
      key.parent.parent.get == Key.init("a:b").get
      key.parent.parent.parent.isErr

    check:
      key.parent.get.path.get == Key.init("a:b").get
      key.path.get == Key.init("a:b/c/d").get
      Key.init("a:b/c").path.get == Key.init("a:b").get
      Key.init("a:b/c/d:e").path.get == Key.init("a:b/c/d").get

    check: key.child(Namespace.init("f:g").get) == Key.init("a:b/c/d:e/f:g").get

    check: key / Namespace.init("f:g").get == Key.init("a:b/c/d:e/f:g").get

    var
      emptyNss: seq[Namespace]

    check:
      key.child(emptyNss) == key
      key.child(Namespace.init("f:g").get, Namespace.init("h:i").get) ==
        Key.init("a:b/c/d:e/f:g/h:i").get

    check:
      key.child(Key.init("f:g").get) == Key.init("a:b/c/d:e/f:g").get
      key / Key.init("f:g").get == Key.init("a:b/c/d:e/f:g").get

    var
      emptyKeys: seq[Key]

    check:
      key.child(emptyKeys) == key
      key.child(Key.init("f:g").get, Key.init("h:i").get) ==
        Key.init("a:b/c/d:e/f:g/h:i").get

    check:
      key.child("f:g", ":::").isErr
      key.child("f:g", "h:i").get == Key.init("a:b/c/d:e/f:g/h:i").get
      key.child("").get == key
      key.child("", "", "").get == key

    check:
      (key / "").get == key
      (key / "f:g").get == Key.init("a:b/c/d:e/f:g").get

    check:
      not key.isAncestorOf(Key.init("f:g").get)
      key.isAncestorOf(key / Key.init("f:g").get)

    check:
      key.isDescendantOf(key.parent.get)
      not Key.init("f:g").get.isDescendantOf(key.parent.get)

  test "serialization":
    let
      idStr = "/a:b/c/d:e"
      key = Key.init(idStr).get

    check:
      key.id == idStr
      $key == "Key(" & key.id & ")"
