import pkg/datastore

template queryTests*(ds: Datastore, testLimitsAndOffsets = true, testSortOrder = true) {.dirty.} =
  var
    key1: Key
    key2: Key
    key3: Key
    val1: seq[byte]
    val2: seq[byte]
    val3: seq[byte]

  setupAll:
    key1 = Key.init("/a").tryGet
    key2 = Key.init("/a/b").tryGet
    key3 = Key.init("/a/b/c").tryGet
    val1 = "value for 1".toBytes
    val2 = "value for 2".toBytes
    val3 = "value for 3".toBytes

  test "Key should query all keys and all it's children":
    let
      q = Query.init(key1)

    (await ds.put(key1, val1)).tryGet
    (await ds.put(key2, val2)).tryGet
    (await ds.put(key3, val3)).tryGet

    let
      iter = (await ds.query(q)).tryGet
      res = (await allFinished(toSeq(iter)))
        .mapIt( it.read.tryGet )
        .filterIt( it.key.isSome )

    check:
      res.len == 3
      res[0].key.get == key1
      res[0].data == val1

      res[1].key.get == key2
      res[1].data == val2

      res[2].key.get == key3
      res[2].data == val3

    (await iter.dispose()).tryGet

  test "Key should query all keys without values":
    let
      q = Query.init(key1, value = false)

    (await ds.put(key1, val1)).tryGet
    (await ds.put(key2, val2)).tryGet
    (await ds.put(key3, val3)).tryGet

    let
      iter = (await ds.query(q)).tryGet
      res = (await allFinished(toSeq(iter)))
        .mapIt( it.read.tryGet )
        .filterIt( it.key.isSome )

    check:
      res.len == 3
      res[0].key.get == key1
      res[0].data.len == 0

      res[1].key.get == key2
      res[1].data.len == 0

      res[2].key.get == key3
      res[2].data.len == 0

    (await iter.dispose()).tryGet

  test "Key should not query parent":
    let
      q = Query.init(key2)

    (await ds.put(key1, val1)).tryGet
    (await ds.put(key2, val2)).tryGet
    (await ds.put(key3, val3)).tryGet

    let
      iter = (await ds.query(q)).tryGet
      res = (await allFinished(toSeq(iter)))
        .mapIt( it.read.tryGet )
        .filterIt( it.key.isSome )

    check:
      res.len == 2
      res[0].key.get == key2
      res[0].data == val2

      res[1].key.get == key3
      res[1].data == val3

    (await iter.dispose()).tryGet

  test "Key should all list all keys at the same level":
    let
      queryKey = Key.init("/a").tryGet
      q = Query.init(queryKey)

    (await ds.put(key1, val1)).tryGet
    (await ds.put(key2, val2)).tryGet
    (await ds.put(key3, val3)).tryGet

    let
      iter = (await ds.query(q)).tryGet

    var
      res = (await allFinished(toSeq(iter)))
        .mapIt( it.read.tryGet )
        .filterIt( it.key.isSome )

    res.sort do (a, b: QueryResponse) -> int:
      cmp(a.key.get.id, b.key.get.id)

    check:
      res.len == 3
      res[0].key.get == key1
      res[0].data == val1

      res[1].key.get == key2
      res[1].data == val2

      res[2].key.get == key3
      res[2].data == val3

    (await iter.dispose()).tryGet

  if testLimitsAndOffsets:
    test "Should apply limit":
      let
        key = Key.init("/a").tryGet
        q = Query.init(key, limit = 10)

      for i in 0..<100:
        let
          key = Key.init(key, Key.init("/" & $i).tryGet).tryGet
          val = ("val " & $i).toBytes

        (await ds.put(key, val)).tryGet

      let
        iter = (await ds.query(q)).tryGet
        res = (await allFinished(toSeq(iter)))
          .mapIt( it.read.tryGet )
          .filterIt( it.key.isSome )

      check:
        res.len == 10

      (await iter.dispose()).tryGet

    test "Should not apply offset":
      let
        key = Key.init("/a").tryGet
        q = Query.init(key, offset = 90)

      for i in 0..<100:
        let
          key = Key.init(key, Key.init("/" & $i).tryGet).tryGet
          val = ("val " & $i).toBytes

        (await ds.put(key, val)).tryGet

      let
        iter = (await ds.query(q)).tryGet
        res = (await allFinished(toSeq(iter)))
          .mapIt( it.read.tryGet )
          .filterIt( it.key.isSome )

      check:
        res.len == 10

      (await iter.dispose()).tryGet

    test "Should not apply offset and limit":
      let
        key = Key.init("/a").tryGet
        q = Query.init(key, offset = 95, limit = 5)

      for i in 0..<100:
        let
          key = Key.init(key, Key.init("/" & $i).tryGet).tryGet
          val = ("val " & $i).toBytes

        (await ds.put(key, val)).tryGet

      let
        iter = (await ds.query(q)).tryGet
        res = (await allFinished(toSeq(iter)))
          .mapIt( it.read.tryGet )
          .filterIt( it.key.isSome )

      check:
        res.len == 5

      for i in 0..<res.high:
        let
          val = ("val " & $(i + 95)).toBytes
          key = Key.init(key, Key.init("/" & $(i + 95)).tryGet).tryGet

        check:
          res[i].key.get == key
          res[i].data == val

      (await iter.dispose()).tryGet

  if testSortOrder:
    test "Should apply sort order - descending":
      let
        key = Key.init("/a").tryGet
        q = Query.init(key, sort = SortOrder.Descending)

      var kvs: seq[QueryResponse]
      for i in 0..<100:
        let
          k = Key.init(key, Key.init("/" & $i).tryGet).tryGet
          val = ("val " & $i).toBytes

        kvs.add((k.some, val))
        (await ds.put(k, val)).tryGet

      # lexicographic sort, as it comes from the backend
      kvs.sort do (a, b: QueryResponse) -> int:
        cmp(a.key.get.id, b.key.get.id)

      kvs = kvs.reversed
      let
        iter = (await ds.query(q)).tryGet
        res = (await allFinished(toSeq(iter)))
          .mapIt( it.read.tryGet )
          .filterIt( it.key.isSome )

      check:
        res.len == 100

      for i, r in res[1..^1]:
        check:
          res[i].key.get == kvs[i].key.get
          res[i].data == kvs[i].data

      (await iter.dispose()).tryGet
