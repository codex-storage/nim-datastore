import std/options

import pkg/unittest2
import pkg/questionable
import pkg/questionable/results

import ../../datastore/key

suite "Namespace":
  test "init failure":
    check:
      Namespace.init("a", "").isFailure
      Namespace.init("a", "   ").isFailure
      Namespace.init("a", ":").isFailure
      Namespace.init("a", "/").isFailure
      Namespace.init(":", "b").isFailure
      Namespace.init("/", "b").isFailure
      Namespace.init("   ", "b").isFailure
      Namespace.init("").isFailure
      Namespace.init("   ").isFailure
      Namespace.init("/").isFailure
      Namespace.init(":").isFailure
      Namespace.init("a:b:c").isFailure
      Namespace.init("a:").isFailure
      Namespace.init("a:   ").isFailure
      Namespace.init("   :b").isFailure

  test "init success":
    check:
      Namespace.init("", "b").isSuccess
      Namespace.init("a", "b").isSuccess
      Namespace.init("a").isSuccess
      Namespace.init("a:b").isSuccess
      Namespace.init(":b").isSuccess

  test "accessors":
    var
      ns: Namespace

    ns = Namespace.init("", "b").tryGet()

    check:
      ns.value == "b"
      ns.field == ""

    ns = Namespace.init("a", "b").tryGet()

    check:
      ns.value == "b"
      ns.field != "" and ns.field == "a"

    ns = Namespace.init(":b").tryGet()

    check:
      ns.value == "b"
      ns.field == ""

    ns = Namespace.init("a:b").tryGet()

    check:
      ns.value == "b"
      ns.field == "a"

  test "equality":
    check:
      Namespace.init("a").tryGet() == Namespace.init("a").tryGet()
      Namespace.init("a").tryGet() != Namespace.init("b").tryGet()
      Namespace.init("", "b").tryGet() == Namespace.init("", "b").tryGet()
      Namespace.init("", "b").tryGet() == Namespace.init("b").tryGet()
      Namespace.init(":b").tryGet() == Namespace.init("b").tryGet()
      Namespace.init("", "b").tryGet() != Namespace.init("", "a").tryGet()
      Namespace.init("", "b").tryGet() != Namespace.init("a").tryGet()
      Namespace.init(":b").tryGet() != Namespace.init("a").tryGet()
      Namespace.init("a", "b").tryGet() == Namespace.init("a", "b").tryGet()
      Namespace.init("a", "b").tryGet() == Namespace.init("a:b").tryGet()
      Namespace.init("a:b").tryGet() == Namespace.init("a:b").tryGet()
      Namespace.init("a", "b").tryGet() != Namespace.init("b", "a").tryGet()
      Namespace.init("a", "b").tryGet() != Namespace.init("b:a").tryGet()
      Namespace.init("a:b").tryGet() != Namespace.init("b:a").tryGet()
      Namespace.init("a").tryGet() != Namespace.init("a:b").tryGet()

  test "serialization":
    var
      ns: Namespace

    ns = Namespace.init(":b").tryGet()

    check:
      ns.id == "b"
      $ns == "Namespace(" & ns.id & ")"

    ns = Namespace.init("a:b").tryGet()

    check:
      ns.id == "a:b"
      $ns == "Namespace(" & ns.id & ")"

suite "Key":
  test "init failure":
    check:
      Key.init("", "").isFailure
      Key.init(@[""]).isFailure
      Key.init(@[":"]).isFailure
      Key.init(@["/"]).isFailure
      Key.init("").isFailure
      Key.init("   ").isFailure
      Key.init("/").isFailure
      Key.init("///").isFailure
      Key.init(":").isFailure
      Key.init("::").isFailure
      Key.init("a:").isFailure
      Key.init("a:b/c:").isFailure

  test "init success":
    check:
      Key.init(Namespace.init("a").tryGet()).isSuccess
      Key.init(@["a:b"]).isSuccess
      Key.init(":b").isSuccess
      Key.init("a:b").isSuccess
      Key.init("a:b/c").isSuccess
      Key.init("a:b/:c").isSuccess
      Key.init("/a:b/c/").isSuccess
      Key.init("///a:b///c///").isSuccess

  test "accessors":
    let
      key = Key.init("/a:b/c/d:e").tryGet()

    check:
      key.namespaces == @[
        Namespace.init("a:b").tryGet(),
        Namespace.init("c").tryGet(),
        Namespace.init("d:e").tryGet()
      ]

      key.list == key.namespaces

  test "equality":
    check:
      Key.init(Namespace.init("a:b").tryGet(), Namespace.init("c").tryGet()).tryGet() == Key.init("a:b/c").tryGet()
      Key.init("a:b", "c").tryGet() == Key.init("a:b/c").tryGet()
      Key.init("a:b/c").tryGet() == Key.init("a:b/c").tryGet()
      Key.init(Namespace.init("a:b").tryGet(), Namespace.init("c").tryGet()).tryGet() != Key.init("c:b/a").tryGet()
      Key.init("a:b", "c").tryGet() != Key.init("c:b/a").tryGet()
      Key.init("a:b/c").tryGet() != Key.init("c:b/a").tryGet()
      Key.init("a:b/c").tryGet() == Key.init("/a:b/c/").tryGet()
      Key.init("a:b/c").tryGet() == Key.init("///a:b///c///").tryGet()
      Key.init("a:b/c").tryGet() != Key.init("///a:b///d///").tryGet()
      Key.init("a").tryGet() != Key.init("a:b").tryGet()
      Key.init("a").tryGet() != Key.init("a/b").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a/b").tryGet()
      Key.init("a:X/b/c").tryGet() == Key.init("a:X/b/c").tryGet()
      Key.init("a/b:X/c").tryGet() == Key.init("a/b:X/c").tryGet()
      Key.init("a/b/c:X").tryGet() == Key.init("a/b/c:X").tryGet()
      Key.init("a:X/b/c:X").tryGet() == Key.init("a:X/b/c:X").tryGet()
      Key.init("a:X/b:X/c").tryGet() == Key.init("a:X/b:X/c").tryGet()
      Key.init("a/b:X/c:X").tryGet() == Key.init("a/b:X/c:X").tryGet()
      Key.init("a:X/b:X/c:X").tryGet() == Key.init("a:X/b:X/c:X").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a:X/b/c").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a/b:X/c").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a/b/c:X").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a:X/b/c:X").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a:X/b:X/c").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a/b:X/c:X").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a:X/b:X/c:X").tryGet()

  test "helpers":
    check: Key.random.len == 24

    let
      key = Key.init("/a:b/c/d:e").tryGet()

    check:
      key[1] == Namespace.init("c").tryGet()
      key[1..^1] == @[Namespace.init("c").tryGet(), Namespace.init("d:e").tryGet()]
      key[^1] == Namespace.init("d:e").tryGet()

    check: key.len == key.namespaces.len

    var
      nss: seq[Namespace]

    for ns in key:
      nss.add ns

    check:
      nss == @[
        Namespace.init("a:b").tryGet(),
        Namespace.init("c").tryGet(),
        Namespace.init("d:e").tryGet()
      ]

    check:
      key.reversed.namespaces == @[
        Namespace.init("d:e").tryGet(),
        Namespace.init("c").tryGet(),
        Namespace.init("a:b").tryGet()
      ]

      key.reverse == key.reversed

    check: key.name == "e"

    check:
      Key.init(":b").tryGet().isTopLevel
      not Key.init(":b/c").tryGet().isTopLevel

    check:
      Key.init(":b").tryGet().parent.isFailure
      Key.init(":b").tryGet().parent.isFailure
      key.parent.tryGet() == Key.init("a:b/c").tryGet()
      key.parent.?parent.tryGet() == Key.init("a:b").tryGet()
      key.parent.?parent.?parent.isFailure

    check:
      key.parent.?path.tryGet() == Key.init("a:b").tryGet()
      key.path.tryGet() == Key.init("a:b/c/d").tryGet()
      Key.init("a:b/c").?path.tryGet() == Key.init("a:b").tryGet()
      Key.init("a:b/c/d:e").?path.tryGet() == Key.init("a:b/c/d").tryGet()

    check: key.child(Namespace.init("f:g").tryGet()) == Key.init("a:b/c/d:e/f:g").tryGet()

    check: key / Namespace.init("f:g").tryGet() == Key.init("a:b/c/d:e/f:g").tryGet()

    var
      emptyNss: seq[Namespace]

    check:
      key.child(emptyNss) == key
      key.child(Namespace.init("f:g").tryGet(), Namespace.init("h:i").tryGet()) ==
        Key.init("a:b/c/d:e/f:g/h:i").tryGet()

    check:
      key.child(Key.init("f:g").tryGet()) == Key.init("a:b/c/d:e/f:g").tryGet()
      key / Key.init("f:g").tryGet() == Key.init("a:b/c/d:e/f:g").tryGet()

    var
      emptyKeys: seq[Key]

    check:
      key.child(emptyKeys) == key
      key.child(Key.init("f:g").tryGet(), Key.init("h:i").tryGet()) ==
        Key.init("a:b/c/d:e/f:g/h:i").tryGet()

    check:
      key.child("f:g", ":::").isFailure
      key.child("f:g", "h:i").tryGet() == Key.init("a:b/c/d:e/f:g/h:i").tryGet()
      key.child("").tryGet() == key
      key.child("", "", "").tryGet() == key

    check:
      (key / "").tryGet() == key
      (key / "f:g").tryGet() == Key.init("a:b/c/d:e/f:g").tryGet()

    check:
      not key.isAncestorOf(Key.init("f:g").tryGet())
      key.isAncestorOf(key / Key.init("f:g").tryGet())

    check:
      key.isDescendantOf(key.parent.tryGet())
      not Key.init("f:g").tryGet().isDescendantOf(key.parent.tryGet())

  test "serialization":
    let
      idStr = "/a:b/c/d:e"
      key = Key.init(idStr).tryGet()

    check:
      key.id == idStr
      $key == "Key(" & key.id & ")"
