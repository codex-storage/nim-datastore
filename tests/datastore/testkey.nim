import std/options
import std/sequtils
import std/algorithm

import pkg/unittest2
import pkg/questionable
import pkg/questionable/results

import ../../datastore/key

suite "Namespace":
  test "should fail init":
    check:
      Namespace.init("a:b:c").isFailure
      Namespace.init(":", "b").isFailure
      Namespace.init("a", ":").isFailure
      Namespace.init("a", "/").isFailure
      Namespace.init("/", "b").isFailure
      Namespace.init("/").isFailure

  test "should succeed":
    check:
      Namespace.init("   :b").isSuccess
      Namespace.init("a:   ").isSuccess
      Namespace.init("a", "").isSuccess
      Namespace.init("a", "   ").isSuccess
      Namespace.init("   ", "b").isSuccess
      Namespace.init("a:").isSuccess
      Namespace.init("").isSuccess
      Namespace.init("   ").isSuccess
      Namespace.init(":").isSuccess
      Namespace.init("", "b").isSuccess
      Namespace.init("a", "b").isSuccess
      Namespace.init("a").isSuccess
      Namespace.init("a:b").isSuccess
      Namespace.init(":b").isSuccess

  test "should init with value":
    let
      ns = Namespace.init("", "b").tryGet()

    check:
      ns.value == "b"
      ns.field == ""

  test "should init with field and value":
    let
      ns = Namespace.init("a", "b").tryGet()

    check:
      ns.value == "b"
      ns.field == "a"

  test "should init with value from id string":
    let
      ns = Namespace.init(":b").tryGet()

    check:
      ns.value == "b"
      ns.field == ""

  test "should init with field and value from id string":
    let
      ns = Namespace.init("a:b").tryGet()

    check:
      ns.value == "b"
      ns.field == "a"

  test "should equal":
    check:
      Namespace.init("a").tryGet() == Namespace.init("a").tryGet()
      Namespace.init("", "b").tryGet() == Namespace.init("", "b").tryGet()
      Namespace.init("", "b").tryGet() == Namespace.init("b").tryGet()
      Namespace.init(":b").tryGet() == Namespace.init("b").tryGet()
      Namespace.init("a", "b").tryGet() == Namespace.init("a", "b").tryGet()
      Namespace.init("a", "b").tryGet() == Namespace.init("a:b").tryGet()
      Namespace.init("a:b").tryGet() == Namespace.init("a:b").tryGet()

  test "should not equal":
    check:
      Namespace.init("a").tryGet() != Namespace.init("b").tryGet()
      Namespace.init("", "b").tryGet() != Namespace.init("", "a").tryGet()
      Namespace.init("", "b").tryGet() != Namespace.init("a").tryGet()
      Namespace.init(":b").tryGet() != Namespace.init("a").tryGet()
      Namespace.init("a", "b").tryGet() != Namespace.init("b", "a").tryGet()
      Namespace.init("a", "b").tryGet() != Namespace.init("b:a").tryGet()
      Namespace.init("a:b").tryGet() != Namespace.init("b:a").tryGet()
      Namespace.init("a").tryGet() != Namespace.init("a:b").tryGet()

  test "should return id from value string":
    let
      ns = Namespace.init(":b").tryGet()

    check:
      ns.id == "b"
      $ns == ns.id

  test "should init id from field and value string":
    let
      ns = Namespace.init("a:b").tryGet()

    check:
      ns.id == "a:b"
      $ns == ns.id

suite "Key":
  test "init failure":

    check:
      Key.init("::").isFailure

  test "init success":
    check:
      Key.init(@["/"]).isSuccess
      Key.init("a:b/c:").isSuccess
      Key.init("a:").isSuccess
      Key.init(":").isSuccess
      Key.init(@[":"]).isSuccess
      Key.init("").isSuccess
      Key.init("   ").isSuccess
      Key.init("/").isSuccess
      Key.init("///").isSuccess
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

  test "should equal":
    check:
      Key.init(Namespace.init("a:b").tryGet(), Namespace.init("c").tryGet()).tryGet() == Key.init("a:b/c").tryGet()
      Key.init("a:b", "c").tryGet() == Key.init("a:b/c").tryGet()
      Key.init("a:b/c").tryGet() == Key.init("a:b/c").tryGet()
      Key.init(Namespace.init("a:b").tryGet(), Namespace.init("c").tryGet()).tryGet() != Key.init("c:b/a").tryGet()
      Key.init("a:b/c").tryGet() == Key.init("/a:b/c/").tryGet()
      Key.init("a:b/c").tryGet() == Key.init("///a:b///c///").tryGet()
      Key.init("a:X/b/c").tryGet() == Key.init("a:X/b/c").tryGet()
      Key.init("a/b:X/c").tryGet() == Key.init("a/b:X/c").tryGet()
      Key.init("a/b/c:X").tryGet() == Key.init("a/b/c:X").tryGet()
      Key.init("a:X/b/c:X").tryGet() == Key.init("a:X/b/c:X").tryGet()
      Key.init("a:X/b:X/c").tryGet() == Key.init("a:X/b:X/c").tryGet()
      Key.init("a/b:X/c:X").tryGet() == Key.init("a/b:X/c:X").tryGet()
      Key.init("a:X/b:X/c:X").tryGet() == Key.init("a:X/b:X/c:X").tryGet()

  test "should not equal":
    check:
      Key.init("a:b", "c").tryGet() != Key.init("c:b/a").tryGet()
      Key.init("a:b/c").tryGet() != Key.init("c:b/a").tryGet()
      Key.init("a:b/c").tryGet() != Key.init("///a:b///d///").tryGet()
      Key.init("a").tryGet() != Key.init("a:b").tryGet()
      Key.init("a").tryGet() != Key.init("a/b").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a/b").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a:X/b/c").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a/b:X/c").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a/b/c:X").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a:X/b/c:X").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a:X/b:X/c").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a/b:X/c:X").tryGet()
      Key.init("a/b/c").tryGet() != Key.init("a:X/b:X/c:X").tryGet()

  test "random key":
    check: Key.random.len == 24

  test "key index":
    let
      key = Key.init("/a:b/c/d:e").tryGet()

    check:
      key[1] == Namespace.init("c").tryGet()
      key[1..^1] == @[Namespace.init("c").tryGet(), Namespace.init("d:e").tryGet()]
      key[^1] == Namespace.init("d:e").tryGet()
      key.len == key.namespaces.len

  test "key iterator":
    let
      key = Key.init("/a:b/c/d:e").tryGet()

    var
      nss = key.mapIt( it )

    check:
      nss == @[
        Namespace.init("a:b").tryGet(),
        Namespace.init("c").tryGet(),
        Namespace.init("d:e").tryGet()
      ]

  test "key reversed":
    let
      key = Key.init("/a:b/c/d:e").tryGet()

    check:
      key.reverse.namespaces == @[
        Namespace.init("d:e").tryGet(),
        Namespace.init("c").tryGet(),
        Namespace.init("a:b").tryGet()
      ]

      key.reverse.namespaces == key.namespaces.reversed

    check:
      key.reverse.value == "b"
      key.reverse.field == "a"

  test "key root":
    let
      key = Key.init("/a:b/c/d:e").tryGet()

    check:
      Key.init(":b").tryGet().root
      not Key.init(":b/c").tryGet().root

  test "key parent":
    let
      key = Key.init("/a:b/c/d:e").tryGet()

    check:
      Key.init(":b").?parent.isFailure
      Key.init(":b").?parent.isFailure

      key.parent.tryGet() == Key.init("a:b/c").tryGet()
      key.parent.?parent.tryGet() == Key.init("a:b").tryGet()
      key.parent.?parent.?parent.isFailure

  test "key path":
    let
      key = Key.init("/a:b/c/d:e").tryGet()

    check:
      key.path.tryGet() == Key.init("/a:b/c/d").tryGet()
      key.parent.?path.tryGet() == Key.init("a:b/c").tryGet()

      Key.init("a:b/c:d").?path.tryGet() == Key.init("a:b/c").tryGet()
      Key.init("a:b/c/d:e").?path.tryGet() == Key.init("a:b/c/d").tryGet()

  test "key child":
    let
      key = Key.init("/a:b/c/d:e").tryGet()

    check:
      key.child(Namespace.init("f:g").tryGet()) == Key.init("a:b/c/d:e/f:g").tryGet()
      key.child(newSeq[Namespace]()) == key
      key.child(
        Namespace.init("f:g").tryGet(),
        Namespace.init("h:i").tryGet()) == Key.init("a:b/c/d:e/f:g/h:i").tryGet()

      key.child(Key.init("f:g").tryGet()) == Key.init("a:b/c/d:e/f:g").tryGet()
      key.child(newSeq[Key]()) == key

      key.child(
        Key.init("f:g").tryGet(),
        Key.init("h:i").tryGet()) == Key.init("a:b/c/d:e/f:g/h:i").tryGet()

      key.child("f:g", ":::").isFailure
      key.child("f:g", "h:i").tryGet() == Key.init("a:b/c/d:e/f:g/h:i").tryGet()
      key.child("").tryGet() == key
      key.child("", "", "").tryGet() == key

  test "key / operator":
    let
      key = Key.init("/a:b/c/d:e").tryGet()

    check:
      key / Namespace.init("f:g").tryGet() == Key.init("a:b/c/d:e/f:g").tryGet()
      (key / "").tryGet() == key
      (key / "f:g").tryGet() == Key.init("a:b/c/d:e/f:g").tryGet()

  test "key ancestor":
    let
      key = Key.init("/a:b/c/d:e").tryGet()

    check:
      not key.ancestor(Key.init("f:g").tryGet())
      key.ancestor(key / Key.init("f:g").tryGet())

  test "key descendant":
    let
      key = Key.init("/a:b/c/d:e").tryGet()

    check:
      key.descendant(key.parent.tryGet())
      not Key.init("f:g").tryGet().descendant(key.parent.tryGet())

  test "key serialization":
    let
      idStr = "/a:b/c/d:e"
      key = Key.init(idStr).tryGet()

    check:
      key.id == idStr
      $key == key.id
