
template testBasicBackend*[K, V, DB](
  ds: DB,
  key: K,
  bytes: V,
  otherBytes: V,
  batch: untyped,
  extended = true
): untyped =

  test "put":
    ds.put(key, bytes).tryGet()

  test "get":
    check:
      ds.get(key).tryGet() == bytes

  test "put update":
    ds.put(key, otherBytes).tryGet()

  test "get updated":
    check:
      ds.get(key).tryGet() == otherBytes

  test "delete":
    ds.delete(key).tryGet()

  test "contains":
    check key notin ds

  test "put batch":

    ds.put(batch).tryGet

    for (k, v) in batch:
      check: ds.has(k).tryGet

  test "delete batch":
    var keys: seq[K]
    for (k, v) in batch:
      keys.add(k)

    ds.delete(keys).tryGet

    for (k, v) in batch:
      check: not ds.has(k).tryGet

  test "handle missing key":
    when K is KeyId:
      let key = KeyId.new Key.init("/missing/key").tryGet().id()
    elif K is string:
      let key = $KeyId.new Key.init("/missing/key").tryGet().id()

    expect(DatastoreKeyNotFound):
      discard ds.get(key).tryGet() # non existing key

template queryTests*(
  dsNew: untyped,
  key1, key2, key3: untyped,
  val1, val2, val3: untyped,
  extended = true
) =

  setup:
    let
      ds = dsNew()
      key1 = key1
      key2 = key2
      key3 = key3
      val1 = val1
      val2 = val2
      val3 = val3

  test "Key should query all keys and all it's children":
    let
      q = dbQuery(key=key1, value=true)

    ds.put(key1, val1).tryGet
    ds.put(key2, val2).tryGet
    ds.put(key3, val3).tryGet

    var
      handle  = ds.query(q).tryGet
      res = handle.iter().toSeq().mapIt(it.tryGet())

    check:
      res.len == 3
      res[0].key.get == key1
      res[0].data == val1

      res[1].key.get == key2
      res[1].data == val2

      res[2].key.get == key3
      res[2].data == val3

  test "query should cancel":
    let
      q = dbQuery(key= key1, value= true)

    ds.put(key1, val1).tryGet
    ds.put(key2, val2).tryGet
    ds.put(key3, val3).tryGet

    var
      handle  = ds.query(q).tryGet
    
    var res: seq[DbQueryResponse[KeyId, DataBuffer]]
    var cnt = 0
    for item in handle.iter():
      cnt.inc
      res.insert(item.tryGet(), 0)
      if cnt > 1:
        handle.cancel = true

    check:
      handle.cancel == true
      handle.closed == true
      res.len == 2

      res[0].key.get == key2
      res[0].data == val2

      res[1].key.get == key1
      res[1].data == val1

  test "Key should query all keys without values":
    let
      q = dbQuery(key= key1, value= false)

    ds.put(key1, val1).tryGet
    ds.put(key2, val2).tryGet
    ds.put(key3, val3).tryGet

    var
      handle  = ds.query(q).tryGet
    let
      res = handle.iter().toSeq().mapIt(it.tryGet())

    check:
      res.len == 3
      res[0].key.get == key1
      res[0].data.len == 0

      res[1].key.get == key2
      res[1].data.len == 0

      res[2].key.get == key3
      res[2].data.len == 0


  test "Key should not query parent":
    let
      q = dbQuery(key= key2, value= true)

    ds.put(key1, val1).tryGet
    ds.put(key2, val2).tryGet
    ds.put(key3, val3).tryGet

    var
      handle  = ds.query(q).tryGet
    let
      res = handle.iter().toSeq().mapIt(it.tryGet())
        # ).filterIt(it.isOk).mapIt(it.tryGet())

    check:
      res.len == 2
      res[0].key.get == key2
      res[0].data == val2

      res[1].key.get == key3
      res[1].data == val3

  test "Key should all list all keys at the same level":
    let
      queryKey = Key.init("/a").tryGet
      q = dbQuery(key= key1, value= true)

    ds.put(key1, val1).tryGet
    ds.put(key2, val2).tryGet
    ds.put(key3, val3).tryGet

    var
      handle  = ds.query(q).tryGet
      res = handle.iter().toSeq().mapIt(it.tryGet())

    res.sort do (a, b: DbQueryResponse[KeyId, DataBuffer]) -> int:
      cmp($a.key.get, $b.key.get)

    check:
      res.len == 3
      res[0].key.get == key1
      res[0].data == val1

      res[1].key.get == key2
      res[1].data == val2

      res[2].key.get == key3
      res[2].data == val3

  if extended:
    test "Should apply limit":
      let
        key = Key.init("/a").tryGet
        q = dbQuery(key= key1, limit= 10, value= false)

      for i in 0..<100:
        let
          key = KeyId.new $Key.init(key, Key.init("/" & $i).tryGet).tryGet
          val = DataBuffer.new("val " & $i)

        ds.put(key, val).tryGet

      var
        handle  = ds.query(q).tryGet
      let
        res = handle.iter().toSeq().mapIt(it.tryGet())

      check:
        res.len == 10

    test "Should not apply offset":
      let
        key = Key.init("/a").tryGet
        keyId = KeyId.new $key
        q = dbQuery(key= keyId, offset= 90)

      for i in 0..<100:
        let
          key = KeyId.new $Key.init(key, Key.init("/" & $i).tryGet).tryGet
          val = DataBuffer.new("val " & $i)

        ds.put(key, val).tryGet

      var
        qr  = ds.query(q)
      # echo "RES: ", qr.repr

      var
        handle  = ds.query(q).tryGet
      let
        res = handle.iter().toSeq().mapIt(it.tryGet())

      # echo "RES: ", res.mapIt(it.key)
      check:
        res.len == 10

    test "Should not apply offset and limit":
      let
        key = Key.init("/a").tryGet
        keyId = KeyId.new $key
        q = dbQuery(key= keyId, offset= 95, limit= 5)

      for i in 0..<100:
        let
          key = KeyId.new $Key.init(key, Key.init("/" & $i).tryGet).tryGet
          val = DataBuffer.new("val " & $i)

        ds.put(key, val).tryGet

      var
        handle  = ds.query(q).tryGet
        res = handle.iter().toSeq().mapIt(it.tryGet())

      check:
        res.len == 5

      for i in 0..<res.high:
        let
          val = DataBuffer.new("val " & $(i + 95))
          key = KeyId.new $Key.init(key, Key.init("/" & $(i + 95)).tryGet).tryGet

        check:
          res[i].key.get == key
          # res[i].data == val

      test "Should apply sort order - descending":
        let
          key = Key.init("/a").tryGet
          keyId = KeyId.new $key
          q = dbQuery(key= keyId, value=true, sort= SortOrder.Descending)

        var kvs: seq[DbQueryResponse[KeyId, DataBuffer]]
        for i in 0..<100:
          let
            k = KeyId.new $Key.init(key, Key.init("/" & $i).tryGet).tryGet
            val = DataBuffer.new ("val " & $i)

          kvs.add((k.some, val))
          ds.put(k, val).tryGet

        # lexicographic sort, as it comes from the backend
        kvs.sort do (a, b: DbQueryResponse[KeyId, DataBuffer]) -> int:
          cmp($a.key.get, $b.key.get)

        kvs = kvs.reversed
        var
          handle  = ds.query(q).tryGet
          res = handle.iter().toSeq().mapIt(it.tryGet())

        check:
          res.len == 100

        for i, r in res[1..^1]:
          check:
            res[i].key.get == kvs[i].key.get
            res[i].data == kvs[i].data