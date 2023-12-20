import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./datastore

proc defaultModifyGetImpl*(
  self: Datastore,
  lock: AsyncLock,
  key: Key,
  fn: ModifyGet
  ): Future[?!seq[byte]] {.async.} =
  # Default implementation, serializes all modify operations using provided lock
  #

  await lock.acquire()

  try:
    without data =? await self.get(key), err:
      if not (err of DatastoreKeyNotFound):
        return failure(err)

    let maybeCurrentData =
      if data.len == 0:
        seq[byte].none
      else:
        data.some

    var
      maybeNewData: ?seq[byte]
      aux: seq[byte]

    try:
      (maybeNewData, aux) = (awaitne fn(maybeCurrentData)).read()
    except CatchableError as err:
      return failure(err)

    if newData =? maybeNewData:
      if err =? (await self.put(key, newData)).errorOption:
        return failure(err)
    elif currentData =? maybeCurrentData:
      if err =? (await self.delete(key)).errorOption:
        return failure(err)

    return aux.success
  finally:
    lock.release()

method defaultModifyImpl*(
  self: Datastore,
  lock: AsyncLock,
  key: Key,
  fn: Modify
  ): Future[?!void] {.async.} =
  proc wrappedFn(maybeValue: ?seq[byte]): Future[(?seq[byte], seq[byte])] {.async.} =
    let res = await fn(maybeValue)
    let ignoredAux = newSeq[byte]()
    return (res, ignoredAux)

  if err =? (await self.defaultModifyGetImpl(lock, key, wrappedFn)).errorOption:
    return failure(err)
  else:
    return success()
