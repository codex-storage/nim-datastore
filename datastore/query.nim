import std/options
import std/algorithm
import pkg/upraises
import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./key
import ./types
import ./databuffer
export options, SortOrder

type

  Query* = object
    key*: Key         # Key to be queried
    value*: bool      # Flag to indicate if data should be returned
    limit*: int       # Max items to return - not available in all backends
    offset*: int      # Offset from which to start querying - not available in all backends
    sort*: SortOrder  # Sort order - not available in all backends

  QueryResponse* = tuple[key: ?Key, data: seq[byte]]
  QueryEndedError* = object of DatastoreError

  GetNext* = proc(): Future[?!QueryResponse] {.upraises: [], gcsafe, closure.}
  IterDispose* = proc(): Future[?!void] {.upraises: [], gcsafe.}
  QueryIter* = ref object
    finished*: bool
    next*: GetNext
    dispose*: IterDispose

proc waitForAllQueryResults*(iter: QueryIter): Future[?!seq[QueryResponse]] {.async.} =
  ## for large blocks this would be *expensive*
  var res: seq[QueryResponse]

  while not iter.finished:
    let val = await iter.next()
    if val.isOk():
      let qr = val.tryGet()
      if qr.key.isSome:
        res.add qr
    else:
      return failure val.error()
  
  await iter.dispose()
  return success res

proc waitForAllQueryResults*(qi: Future[?!QueryIter]): Future[?!seq[QueryResponse]] {.async.} =
  without iter =? (await qi), err:
    return failure err
  await waitForAllQueryResults(iter)


proc defaultDispose(): Future[?!void] {.upraises: [], gcsafe, async.} =
  return success()

proc new*(T: type QueryIter, dispose = defaultDispose): T =
  QueryIter(dispose: dispose)

proc init*(
  T: type Query,
  key: Key,
  value = true,
  sort = Ascending,
  offset = 0,
  limit = -1): T =

  T(
    key: key,
    value: value,
    sort: sort,
    offset: offset,
    limit: limit)

type

  QueryBuffer* = object
    key*: KeyBuffer    # Key to be queried
    value*: bool       # Flag to indicate if data should be returned
    limit*: int        # Max items to return - not available in all backends
    offset*: int       # Offset from which to start querying - not available in all backends
    sort*: SortOrder  # Sort order - not available in all backends

  QueryResponseBuffer* = object
    key*: KeyBuffer
    data*: ValueBuffer

  # GetNext* = proc(): Future[?!QueryResponse] {.upraises: [], gcsafe, closure.}
  # IterDispose* = proc(): Future[?!void] {.upraises: [], gcsafe.}
  # QueryIter* = ref object
  #   finished*: bool
  #   next*: GetNext
  #   dispose*: IterDispose

proc toBuffer*(q: Query): QueryBuffer =
  ## convert Query to thread-safe QueryBuffer
  return QueryBuffer(
    key: KeyBuffer.new(q.key),
    value: q.value,
    offset: q.offset,
    sort: q.sort
  )

proc toQuery*(qb: QueryBuffer): Query =
  ## convert QueryBuffer to regular Query
  Query(
    key: qb.key.toKey().expect("key expected"),
    value: qb.value,
    limit: qb.limit,
    offset: qb.offset,
    sort: qb.sort
  )

proc toBuffer*(q: QueryResponse): QueryResponseBuffer =
  ## convert QueryReponses to thread safe type
  var kb: KeyBuffer
  if q.key.isSome():
    kb = KeyBuffer.new(q.key.get())
  var kv: KeyBuffer
  if q.data.len() > 0:
    kv = ValueBuffer.new(q.data)

  QueryResponseBuffer(key: kb, data: kv)

proc toQueryResponse*(qb: QueryResponseBuffer): QueryResponse =
  ## convert QueryReponseBuffer to regular QueryResponse
  let key =
    if qb.key.isNil: none(Key)
    else: some qb.key.toKey().expect("key response should work")
  let data =
    if qb.data.isNil: EmptyBytes
    else: qb.data.toSeq(byte)

  (key: key, data: data)

# proc convert*(ret: TResult[QueryResponseBuffer],
#               tp: typedesc[QueryResponse]
#               ): Result[QueryResponse, ref CatchableError] =
#   if ret[].results.isOk():
#     result.ok(ret[].results.get().toString())
#   else:
#     let exc: ref CatchableError = ret[].results.error().toCatchable()
#     result.err(exc)
