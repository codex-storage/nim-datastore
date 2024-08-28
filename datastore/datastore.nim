{.push raises: [].}

import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./key
import ./query
import ./types

export key, query, types

type
  BatchEntry* = tuple[key: Key, data: seq[byte]]
  Function*[T, U] = proc(value: T): U {.raises: [CatchableError], gcsafe, closure.}
  Modify* = Function[?seq[byte], Future[?seq[byte]]]
  ModifyGet* = Function[?seq[byte], Future[(?seq[byte], seq[byte])]]

method has*(self: Datastore, key: Key): Future[?!bool] {.base, locks: "unknown".} =
  raiseAssert("Not implemented!")

method delete*(self: Datastore, key: Key): Future[?!void] {.base, locks: "unknown".} =
  raiseAssert("Not implemented!")

method delete*(self: Datastore, keys: seq[Key]): Future[?!void] {.base, locks: "unknown".} =
  raiseAssert("Not implemented!")

method get*(self: Datastore, key: Key): Future[?!seq[byte]] {.base, locks: "unknown".} =
  raiseAssert("Not implemented!")

method put*(self: Datastore, key: Key, data: seq[byte]): Future[?!void] {.base, locks: "unknown".} =
  raiseAssert("Not implemented!")

method put*(self: Datastore, batch: seq[BatchEntry]): Future[?!void] {.base, locks: "unknown".} =
  raiseAssert("Not implemented!")

method close*(self: Datastore): Future[?!void] {.base, async, locks: "unknown".} =
  raiseAssert("Not implemented!")

method query*(
  self: Datastore,
  query: Query): Future[?!QueryIter] {.base, gcsafe.} =

  raiseAssert("Not implemented!")

proc contains*(self: Datastore, key: Key): Future[bool] {.async.} =
  return (await self.has(key)) |? false

method modify*(self: Datastore, key: Key, fn: Modify): Future[?!void] {.base, locks: "unknown".} =
  ## Concurrently safe way of modifying the value associated with the `key`.
  ##
  ## Same as `modifyGet`, but this takes `fn` that doesn't produce any auxillary value.
  ##

  raiseAssert("Not implemented!")

method modifyGet*(self: Datastore, key: Key, fn: ModifyGet): Future[?!seq[byte]] {.base, locks: "unknown".} =
  ## Concurrently safe way of updating value associated with the `key`. Returns auxillary value on
  ## successful update.
  ##
  ## This method first reads a value stored under the `key`, if such value exists it's wrapped into `some`
  ## and passed as the only arg to the `fn`, otherwise `none` is passed.
  ##
  ## Table below presents four possibilities of execution. `curr` represents a value passed to `fn`,
  ## while `fn(curr)` represents a value returned by calling `fn` (auxillary value is omitted for clarity).
  ##
  ## | curr    | fn(curr) | action                       |
  ## |---------|----------|------------------------------|
  ## | none    | none     | no action                    |
  ## | none    | some(v)  | insert v                     |
  ## | some(u) | none     | delete u                     |
  ## | some(u) | some(v)  | replace u with v (if u != v) |
  ##
  ## If `fn` raises an error, the value associated with `key` remains unchanged and raised error is wrapped
  ## into a failure and returned as a result.
  ##
  ## Note that `fn` can be called multiple times (when concurrent modification was detected). In such case
  ## only the last auxillary value is returned.
  ##

  raiseAssert("Not implemented!")
