import databuffer

type
  SimpleTable*[N: static int] = object
    ## very simple table that doesn't
    ## use GC types
    data*: array[N, tuple[used: bool, key: KeyBuffer, val: ValueBuffer]]

proc hasKey*[N](table: var SimpleTable[N], key: KeyBuffer): bool =
  for (u, k, _) in table.data:
    if u and key == k:
      return true

proc `[]`*[N](table: var SimpleTable[N], key: KeyBuffer): ValueBuffer {.raises: [KeyError].} =
  for item in table.data:
    if item.used and item.key == key:
      return item.val
  raise newException(KeyError, "no such key")

proc `[]=`*[N](table: var SimpleTable[N], key: KeyBuffer, value: ValueBuffer) =
  for item in table.data.mitems():
    if item.key == key:
      item = (true, key, value)
      return
  # key not found, find free item
  for item in table.data.mitems():
    if item.used == false:
      item = (true, key, value)
      return

proc clear*[N](table: var SimpleTable[N]) =
  for item in table.data.mitems():
    item.used = false

proc pop*[N](table: var SimpleTable[N], key: KeyBuffer, value: var ValueBuffer): bool =
  for item in table.data.mitems():
    if item.used and item.key == key:
      value = item.val
      item.used = false
      return true

iterator keys*[N](table: var SimpleTable[N]): KeyBuffer =
  for (u, k, _) in table.data:
    if u:
      yield k

when isMainModule:
  import unittest2

  suite "simple table":

    var table: SimpleTable[10]
    let k1 = KeyBuffer.new("k1")
    let k2 = KeyBuffer.new("k2")
    let v1 = ValueBuffer.new("hello world!")
    let v2 = ValueBuffer.new("other val")

    test "put":
      table[k1] = v1
      table[k2] = v2
    test "hasKey":
      check table.hasKey(k1)
      check table.hasKey(k2)
    test "get":
      check table[k1].toString == "hello world!"
      check table[k2].toString == "other val"
    test "delete":
      var res: ValueBuffer
      check table.pop(k1, res)
      check res.toString == "hello world!"
      expect KeyError:
        let res = table[k1]
        check res.toString == "hello world!"
    test "put new":
      table[k1] = v1
      table[k1] = v2
      let res = table[k1]
      check res.toString == "other val"