
const
  FileExt* = "dsobj"
  EmptyBytes* = newSeq[byte](0)

type
  DatastoreError* = object of CatchableError
  DatastoreKeyNotFound* = object of DatastoreError
  QueryEndedError* = object of DatastoreError

  Datastore* = ref object of RootObj
