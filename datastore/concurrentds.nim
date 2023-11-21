import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/upraises

import ./key
import ./query
import ./types
import ./datastore

export key, query, types, datastore

push: {.upraises: [].}

type
  Function*[T, U] = proc(value: T): U {.upraises: [CatchableError], gcsafe, closure.}
  Modify* = Function[?seq[byte], ?seq[byte]]
  ModifyGet* = Function[?seq[byte], (?seq[byte], seq[byte])]
  ModifyAsync* = Function[?seq[byte], Future[?seq[byte]]]
  ModifyGetAsync* = Function[?seq[byte], Future[(?seq[byte], seq[byte])]]

method modify*(self: ConcurrentDatastore, key: Key, fn: Modify): Future[?!void] {.base, locks: "unknown".} =
  ## Concurrently safe way of modifying the value associated with the `key`.
  ##
  ## Same as `modifyGet` with `fn: ModifyGetAsync` argument, but this takes non-async `fn` that doesn't
  ## produce any auxillary value.
  ##

  raiseAssert("Not implemented!")

method modify*(self: ConcurrentDatastore, key: Key, fn: ModifyAsync): Future[?!void] {.base, locks: "unknown".} =
  ## Concurrently safe way of modifying the value associated with the `key`.
  ##
  ## Same as `modifyGet` with `fn: ModifyGetAsync` argument, but this takes `fn` that doesn't produce
  ## any auxillary value.
  ##

  raiseAssert("Not implemented!")

method modifyGet*(self: ConcurrentDatastore, key: Key, fn: ModifyGet): Future[?!seq[byte]] {.base, locks: "unknown".} =
  ## Concurrently safe way of updating value associated with the `key`. Returns auxillary value on
  ## successful update.
  ##
  ## Same as `modifyGet` with `fn: ModifyGetAsync` argument, but this takes non-async `fn`.
  ##

  raiseAssert("Not implemented!")

method modifyGet*(self: ConcurrentDatastore, key: Key, fn: ModifyGetAsync): Future[?!seq[byte]] {.base, locks: "unknown".} =
  ## Concurrently safe way of updating value associated with the `key`. Returns auxillary value on
  ## successful update.
  ##
  ## This method first reads a value stored under the `key`, if such value exists it's wrapped into `some`
  ## and passed as the only arg to the `fn`, otherwise `none` is passed.
  ##
  ## When `fn` returns `some`, returned value is put into the store, but only if it's different than
  ## the existing value, otherwise nothing happens.
  ## When `fn` returns `none` existing value is deleted from the store, if no value existed before
  ## nothing happens.
  ##
  ## Note that `fn` can be called multiple times (when concurrent modify of the value was detected). Only the
  ## last auxillary value is returned.
  ##

  raiseAssert("Not implemented!")
