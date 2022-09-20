type
  DatastoreError* = object of CatchableError
  DatastoreKeyNotFound* = object of DatastoreError

  Datastore* = ref object of RootObj
