import pkg/questionable/results
import pkg/upraises

import threads/databuffer

push: {.upraises: [].}

type
  Datastore2*[T] = object
    has*: proc(self: SharedPtr[T], key: KeyBuffer): ?!bool {.nimcall, gcsafe, raises: [].}
    delete*: proc(self: SharedPtr[T], key: KeyBuffer): ?!void {.nimcall, gcsafe, raises: [].}
    get*: proc(self: SharedPtr[T], key: KeyBuffer): ?!ValueBuffer {.nimcall, gcsafe, raises: [].}
    put*: proc(self: SharedPtr[T], key: KeyBuffer, data: ValueBuffer): ?!void {.nimcall, gcsafe, raises: [].}
    close*: proc(self: SharedPtr[T]): ?!void {.gcsafe, raises: [].}
    ids*: SharedPtr[T]

proc has*[T](self: Datastore2[T], key: KeyBuffer): ?!bool =
  self.has(self.ids, key)
proc delete*[T](self: Datastore2[T], key: KeyBuffer): ?!void {.nimcall.} =
  self.delete(self.ids, key)
proc get*[T](self: Datastore2[T], key: KeyBuffer): ?!ValueBuffer {.nimcall.} =
  self.get(self.ids, key)
proc put*[T](self: Datastore2[T], key: KeyBuffer, data: ValueBuffer): ?!void {.nimcall.} =
  self.put(self.ids, key, data)
proc close*[T](self: Datastore2[T]): ?!void {.nimcall.} =
  echo "CLOSE: ", self
  echo "CLOSE: ", self.ids.repr
  self.close(self.ids)
