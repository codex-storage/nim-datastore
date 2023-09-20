import pkg/questionable/results
import pkg/upraises

import threads/databuffer

push: {.upraises: [].}

type
  Datastore2* = object of RootObj
    has*: proc(self: Datastore2, key: KeyBuffer): ?!bool {.nimcall.}
    delete*: proc(self: Datastore2, key: KeyBuffer): ?!void {.nimcall.}
    get*: proc(self: Datastore2, key: KeyBuffer): ?!ValueBuffer {.nimcall.}
    put*: proc(self: Datastore2, key: KeyBuffer, data: ValueBuffer): ?!void {.nimcall.}
    close*: proc(self: Datastore2): ?!void {.nimcall.}

