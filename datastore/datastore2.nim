import pkg/questionable/results
import pkg/upraises

import threads/databuffer

push: {.upraises: [].}

type
  Datastore2*[T] = object of RootObj
    has*: proc(self: var T, key: KeyBuffer): ?!bool {.nimcall, gcsafe, raises: [].}
    delete*: proc(self: var T, key: KeyBuffer): ?!void {.nimcall.}
    get*: proc(self: var T, key: KeyBuffer): ?!ValueBuffer {.nimcall.}
    put*: proc(self: var T, key: KeyBuffer, data: ValueBuffer): ?!void {.nimcall.}
    close*: proc(self: var T): ?!void {.nimcall.}
    ds*: T

proc has*[T](self: var Datastore2[T], key: KeyBuffer): ?!bool =
  self.has(self.ds, key)

proc delete*[T](self: var Datastore2[T], key: KeyBuffer): ?!void {.nimcall.} =
  self.delete(self.ds, key)
proc get*[T](self: var Datastore2[T], key: KeyBuffer): ?!ValueBuffer {.nimcall.} =
  self.get(self.ds, key)
proc put*[T](self: var Datastore2[T], key: KeyBuffer, data: ValueBuffer): ?!void {.nimcall.} =
  self.put(self.ds, key, data)
proc close*[T](self: var Datastore2[T]): ?!void {.nimcall.} =
  self.close(self.ds)
