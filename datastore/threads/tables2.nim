#
#
#            Nim's Runtime Library
#        (c) Copyright 2015 Andreas Rumpf
#
#    See the file "copying.txt", included in this
#    distribution, for details about the copyright.
#

## The `tables` module implements variants of an efficient `hash table`:idx:
## (also often named `dictionary`:idx: in other programming languages) that is
## a mapping from keys to values.
##
## There are several different types of hash tables available:
## * `Table<#Table>`_ is the usual hash table,
## * `OrderedTable<#OrderedTable>`_ is like `Table` but remembers insertion order,
## * `CountTable<#CountTable>`_ is a mapping from a key to its number of occurrences
##
## For consistency with every other data type in Nim these have **value**
## semantics, this means that `=` performs a copy of the hash table.
##
## For `ref semantics<manual.html#types-reference-and-pointer-types>`_
## use their `Ref` variants: `TableRef<#TableRef>`_,
## `OrderedTableRef<#OrderedTableRef>`_, and `CountTableRef<#CountTableRef>`_.
##
## To give an example, when `a` is a `Table`, then `var b = a` gives `b`
## as a new independent table. `b` is initialised with the contents of `a`.
## Changing `b` does not affect `a` and vice versa:

runnableExamples:
  var
    a = {1: "one", 2: "two"}.toTable  # creates a Table
    b = a

  assert a == b

  b[3] = "three"
  assert 3 notin a
  assert 3 in b
  assert a != b

## On the other hand, when `a` is a `TableRef` instead, then changes to `b`
## also affect `a`. Both `a` and `b` **ref** the same data structure:

runnableExamples:
  var
    a = {1: "one", 2: "two"}.newTable  # creates a TableRef
    b = a

  assert a == b

  b[3] = "three"

  assert 3 in a
  assert 3 in b
  assert a == b

##
## ----
##

## # Basic usage


## ## Table
runnableExamples:
  from std/sequtils import zip

  let
    names = ["John", "Paul", "George", "Ringo"]
    years = [1940, 1942, 1943, 1940]

  var beatles = initTable[string, int]()

  for pairs in zip(names, years):
    let (name, birthYear) = pairs
    beatles[name] = birthYear

  assert beatles == {"George": 1943, "Ringo": 1940, "Paul": 1942, "John": 1940}.toTable


  var beatlesByYear = initTable[int, seq[string]]()

  for pairs in zip(years, names):
    let (birthYear, name) = pairs
    if not beatlesByYear.hasKey(birthYear):
      # if a key doesn't exist, we create one with an empty sequence
      # before we can add elements to it
      beatlesByYear[birthYear] = @[]
    beatlesByYear[birthYear].add(name)

  assert beatlesByYear == {1940: @["John", "Ringo"], 1942: @["Paul"], 1943: @["George"]}.toTable

## ## OrderedTable
## `OrderedTable<#OrderedTable>`_ is used when it is important to preserve
## the insertion order of keys.

runnableExamples:
  let
    a = [('z', 1), ('y', 2), ('x', 3)]
    ot = a.toOrderedTable  # ordered tables

  assert $ot == """{'z': 1, 'y': 2, 'x': 3}"""

## ## CountTable
## `CountTable<#CountTable>`_ is useful for counting number of items of some
## container (e.g. string, sequence or array), as it is a mapping where the
## items are the keys, and their number of occurrences are the values.
## For that purpose `toCountTable proc<#toCountTable,openArray[A]>`_
## comes handy:

runnableExamples:
  let myString = "abracadabra"
  let letterFrequencies = toCountTable(myString)
  assert $letterFrequencies == "{'a': 5, 'd': 1, 'b': 2, 'r': 2, 'c': 1}"

## The same could have been achieved by manually iterating over a container
## and increasing each key's value with `inc proc
## <#inc,CountTable[A],A,int>`_:

runnableExamples:
  let myString = "abracadabra"
  var letterFrequencies = initCountTable[char]()
  for c in myString:
    letterFrequencies.inc(c)
  assert $letterFrequencies == "{'d': 1, 'r': 2, 'c': 1, 'a': 5, 'b': 2}"

##
## ----
##

## ## Hashing
##
## If you are using simple standard types like `int` or `string` for the
## keys of the table you won't have any problems, but as soon as you try to use
## a more complex object as a key you will be greeted by a strange compiler
## error:
##
## .. code::
##
##   Error: type mismatch: got (Person)
##   but expected one of:
##   hashes.hash(x: openArray[A]): Hash
##   hashes.hash(x: int): Hash
##   hashes.hash(x: float): Hash
##   …
##
## What is happening here is that the types used for table keys require to have
## a `hash()` proc which will convert them to a `Hash <hashes.html#Hash>`_
## value, and the compiler is listing all the hash functions it knows.
## Additionally there has to be a `==` operator that provides the same
## semantics as its corresponding `hash` proc.
##
## After you add `hash` and `==` for your custom type everything will work.
## Currently, however, `hash` for objects is not defined, whereas
## `system.==` for objects does exist and performs a "deep" comparison (every
## field is compared) which is usually what you want. So in the following
## example implementing only `hash` suffices:

runnableExamples:
  import std/hashes

  type
    Person = object
      firstName, lastName: string

  proc hash(x: Person): Hash =
    ## Piggyback on the already available string hash proc.
    ##
    ## Without this proc nothing works!
    result = x.firstName.hash !& x.lastName.hash
    result = !$result

  var
    salaries = initTable[Person, int]()
    p1, p2: Person

  p1.firstName = "Jon"
  p1.lastName = "Ross"
  salaries[p1] = 30_000

  p2.firstName = "소진"
  p2.lastName = "박"
  salaries[p2] = 45_000

##
## ----
##

## # See also
##
## * `json module<json.html>`_ for table-like structure which allows
##   heterogeneous members
## * `sharedtables module<sharedtables.html>`_ for shared hash table support
## * `strtabs module<strtabs.html>`_ for efficient hash tables
##   mapping from strings to strings
## * `hashes module<hashes.html>`_ for helper functions for hashing


import std/private/since
import hashes, math, algorithm

type
  KeyValuePair[A, B] = tuple[hcode: Hash, key: A, val: B]
  KeyValuePairSeq[A, B] = seq[KeyValuePair[A, B]]
  Table*[A, B] = object
    ## Generic hash table, consisting of a key-value pair.
    ##
    ## `data` and `counter` are internal implementation details which
    ## can't be accessed.
    ##
    ## For creating an empty Table, use `initTable proc<#initTable>`_.
    data: KeyValuePairSeq[A, B]
    counter: int
  TableRef*[A, B] = ref Table[A, B] ## Ref version of `Table<#Table>`_.
    ##
    ## For creating a new empty TableRef, use `newTable proc
    ## <#newTable>`_.

const
  defaultInitialSize* = 32

# ------------------------------ helpers ---------------------------------

# Do NOT move these to tableimpl.nim, because sharedtables uses that
# file and has its own implementation.
template maxHash(t): untyped = high(t.data)
template dataLen(t): untyped = len(t.data)

include tableimpl

proc raiseKeyError[T](key: T) {.noinline, noreturn.} =
  when compiles($key):
    raise newException(KeyError, "key not found: " & $key)
  else:
    raise newException(KeyError, "key not found")

template get(t, key): untyped =
  ## retrieves the value at `t[key]`. The value can be modified.
  ## If `key` is not in `t`, the `KeyError` exception is raised.
  mixin rawGet
  var hc: Hash
  var index = rawGet(t, key, hc)
  if index >= 0: result = t.data[index].val
  else:
    raiseKeyError(key)

proc enlarge[A, B](t: var Table[A, B]) =
  var n: KeyValuePairSeq[A, B]
  newSeq(n, len(t.data) * growthFactor)
  swap(t.data, n)
  for i in countup(0, high(n)):
    let eh = n[i].hcode
    if isFilled(eh):
      var j: Hash = eh and maxHash(t)
      while isFilled(t.data[j].hcode):
        j = nextTry(j, maxHash(t))
      when defined(js):
        rawInsert(t, t.data, n[i].key, n[i].val, eh, j)
      else:
        rawInsert(t, t.data, move n[i].key, move n[i].val, eh, j)




# -------------------------------------------------------------------
# ------------------------------ Table ------------------------------
# -------------------------------------------------------------------

proc initTable*[A, B](initialSize = defaultInitialSize): Table[A, B] =
  ## Creates a new hash table that is empty.
  ##
  ## Starting from Nim v0.20, tables are initialized by default and it is
  ## not necessary to call this function explicitly.
  ##
  ## See also:
  ## * `toTable proc<#toTable,openArray[]>`_
  ## * `newTable proc<#newTable>`_ for creating a `TableRef`
  runnableExamples:
    let
      a = initTable[int, string]()
      b = initTable[char, seq[int]]()
  initImpl(result, initialSize)

proc `[]=`*[A, B](t: var Table[A, B], key: A, val: sink B) =
  ## Inserts a `(key, value)` pair into `t`.
  ##
  ## See also:
  ## * `[] proc<#[],Table[A,B],A>`_ for retrieving a value of a key
  ## * `hasKeyOrPut proc<#hasKeyOrPut,Table[A,B],A,B>`_
  ## * `mgetOrPut proc<#mgetOrPut,Table[A,B],A,B>`_
  ## * `del proc<#del,Table[A,B],A>`_ for removing a key from the table
  runnableExamples:
    var a = initTable[char, int]()
    a['x'] = 7
    a['y'] = 33
    doAssert a == {'x': 7, 'y': 33}.toTable

  putImpl(enlarge)

proc toTable*[A, B](pairs: openArray[(A, B)]): Table[A, B] =
  ## Creates a new hash table that contains the given `pairs`.
  ##
  ## `pairs` is a container consisting of `(key, value)` tuples.
  ##
  ## See also:
  ## * `initTable proc<#initTable>`_
  ## * `newTable proc<#newTable,openArray[]>`_ for a `TableRef` version
  runnableExamples:
    let a = [('a', 5), ('b', 9)]
    let b = toTable(a)
    assert b == {'a': 5, 'b': 9}.toTable

  result = initTable[A, B](pairs.len)
  for key, val in items(pairs): result[key] = val

proc `[]`*[A, B](t: Table[A, B], key: A): B =
  ## Retrieves the value at `t[key]`.
  ##
  ## If `key` is not in `t`, the `KeyError` exception is raised.
  ## One can check with `hasKey proc<#hasKey,Table[A,B],A>`_ whether
  ## the key exists.
  ##
  ## See also:
  ## * `getOrDefault proc<#getOrDefault,Table[A,B],A>`_ to return
  ##   a default value (e.g. zero for int) if the key doesn't exist
  ## * `getOrDefault proc<#getOrDefault,Table[A,B],A,B>`_ to return
  ##   a custom value if the key doesn't exist
  ## * `[]= proc<#[]=,Table[A,B],A,sinkB>`_ for inserting a new
  ##   (key, value) pair in the table
  ## * `hasKey proc<#hasKey,Table[A,B],A>`_ for checking if a key is in
  ##   the table
  runnableExamples:
    let a = {'a': 5, 'b': 9}.toTable
    doAssert a['a'] == 5
    doAssertRaises(KeyError):
      echo a['z']
  get(t, key)

proc `[]`*[A, B](t: var Table[A, B], key: A): var B =
  ## Retrieves the value at `t[key]`. The value can be modified.
  ##
  ## If `key` is not in `t`, the `KeyError` exception is raised.
  ##
  ## See also:
  ## * `getOrDefault proc<#getOrDefault,Table[A,B],A>`_ to return
  ##   a default value (e.g. zero for int) if the key doesn't exist
  ## * `getOrDefault proc<#getOrDefault,Table[A,B],A,B>`_ to return
  ##   a custom value if the key doesn't exist
  ## * `[]= proc<#[]=,Table[A,B],A,sinkB>`_ for inserting a new
  ##   (key, value) pair in the table
  ## * `hasKey proc<#hasKey,Table[A,B],A>`_ for checking if a key is in
  ##   the table
  get(t, key)

proc hasKey*[A, B](t: Table[A, B], key: A): bool =
  ## Returns true if `key` is in the table `t`.
  ##
  ## See also:
  ## * `contains proc<#contains,Table[A,B],A>`_ for use with the `in` operator
  ## * `[] proc<#[],Table[A,B],A>`_ for retrieving a value of a key
  ## * `getOrDefault proc<#getOrDefault,Table[A,B],A>`_ to return
  ##   a default value (e.g. zero for int) if the key doesn't exist
  ## * `getOrDefault proc<#getOrDefault,Table[A,B],A,B>`_ to return
  ##   a custom value if the key doesn't exist
  runnableExamples:
    let a = {'a': 5, 'b': 9}.toTable
    doAssert a.hasKey('a') == true
    doAssert a.hasKey('z') == false

  var hc: Hash
  result = rawGet(t, key, hc) >= 0

proc contains*[A, B](t: Table[A, B], key: A): bool =
  ## Alias of `hasKey proc<#hasKey,Table[A,B],A>`_ for use with
  ## the `in` operator.
  runnableExamples:
    let a = {'a': 5, 'b': 9}.toTable
    doAssert 'b' in a == true
    doAssert a.contains('z') == false

  return hasKey[A, B](t, key)

proc hasKeyOrPut*[A, B](t: var Table[A, B], key: A, val: B): bool =
  ## Returns true if `key` is in the table, otherwise inserts `value`.
  ##
  ## See also:
  ## * `hasKey proc<#hasKey,Table[A,B],A>`_
  ## * `[] proc<#[],Table[A,B],A>`_ for retrieving a value of a key
  ## * `getOrDefault proc<#getOrDefault,Table[A,B],A>`_ to return
  ##   a default value (e.g. zero for int) if the key doesn't exist
  ## * `getOrDefault proc<#getOrDefault,Table[A,B],A,B>`_ to return
  ##   a custom value if the key doesn't exist
  runnableExamples:
    var a = {'a': 5, 'b': 9}.toTable
    if a.hasKeyOrPut('a', 50):
      a['a'] = 99
    if a.hasKeyOrPut('z', 50):
      a['z'] = 99
    doAssert a == {'a': 99, 'b': 9, 'z': 50}.toTable

  hasKeyOrPutImpl(enlarge)

proc getOrDefault*[A, B](t: Table[A, B], key: A): B =
  ## Retrieves the value at `t[key]` if `key` is in `t`. Otherwise, the
  ## default initialization value for type `B` is returned (e.g. 0 for any
  ## integer type).
  ##
  ## See also:
  ## * `[] proc<#[],Table[A,B],A>`_ for retrieving a value of a key
  ## * `hasKey proc<#hasKey,Table[A,B],A>`_
  ## * `hasKeyOrPut proc<#hasKeyOrPut,Table[A,B],A,B>`_
  ## * `mgetOrPut proc<#mgetOrPut,Table[A,B],A,B>`_
  ## * `getOrDefault proc<#getOrDefault,Table[A,B],A,B>`_ to return
  ##   a custom value if the key doesn't exist
  runnableExamples:
    let a = {'a': 5, 'b': 9}.toTable
    doAssert a.getOrDefault('a') == 5
    doAssert a.getOrDefault('z') == 0

  getOrDefaultImpl(t, key)

proc getOrDefault*[A, B](t: Table[A, B], key: A, default: B): B =
  ## Retrieves the value at `t[key]` if `key` is in `t`.
  ## Otherwise, `default` is returned.
  ##
  ## See also:
  ## * `[] proc<#[],Table[A,B],A>`_ for retrieving a value of a key
  ## * `hasKey proc<#hasKey,Table[A,B],A>`_
  ## * `hasKeyOrPut proc<#hasKeyOrPut,Table[A,B],A,B>`_
  ## * `mgetOrPut proc<#mgetOrPut,Table[A,B],A,B>`_
  ## * `getOrDefault proc<#getOrDefault,Table[A,B],A>`_ to return
  ##   a default value (e.g. zero for int) if the key doesn't exist
  runnableExamples:
    let a = {'a': 5, 'b': 9}.toTable
    doAssert a.getOrDefault('a', 99) == 5
    doAssert a.getOrDefault('z', 99) == 99

  getOrDefaultImpl(t, key, default)

proc mgetOrPut*[A, B](t: var Table[A, B], key: A, val: B): var B =
  ## Retrieves value at `t[key]` or puts `val` if not present, either way
  ## returning a value which can be modified.
  ##
  ##
  ## Note that while the value returned is of type `var B`,
  ## it is easy to accidentally create an copy of the value at `t[key]`.
  ## Remember that seqs and strings are value types, and therefore
  ## cannot be copied into a separate variable for modification.
  ## See the example below.
  ##
  ## See also:
  ## * `[] proc<#[],Table[A,B],A>`_ for retrieving a value of a key
  ## * `hasKey proc<#hasKey,Table[A,B],A>`_
  ## * `hasKeyOrPut proc<#hasKeyOrPut,Table[A,B],A,B>`_
  ## * `getOrDefault proc<#getOrDefault,Table[A,B],A>`_ to return
  ##   a default value (e.g. zero for int) if the key doesn't exist
  ## * `getOrDefault proc<#getOrDefault,Table[A,B],A,B>`_ to return
  ##   a custom value if the key doesn't exist
  runnableExamples:
    var a = {'a': 5, 'b': 9}.toTable
    doAssert a.mgetOrPut('a', 99) == 5
    doAssert a.mgetOrPut('z', 99) == 99
    doAssert a == {'a': 5, 'b': 9, 'z': 99}.toTable

    # An example of accidentally creating a copy
    var t = initTable[int, seq[int]]()
    # In this example, we expect t[10] to be modified,
    # but it is not.
    var copiedSeq = t.mgetOrPut(10, @[10])
    copiedSeq.add(20)
    doAssert t[10] == @[10]
    # Correct
    t.mgetOrPut(25, @[25]).add(35)
    doAssert t[25] == @[25, 35]

  mgetOrPutImpl(enlarge)

proc len*[A, B](t: Table[A, B]): int =
  ## Returns the number of keys in `t`.
  runnableExamples:
    let a = {'a': 5, 'b': 9}.toTable
    doAssert len(a) == 2

  result = t.counter

proc add*[A, B](t: var Table[A, B], key: A, val: sink B) {.deprecated:
    "Deprecated since v1.4; it was more confusing than useful, use `[]=`".} =
  ## Puts a new `(key, value)` pair into `t` even if `t[key]` already exists.
  ##
  ## **This can introduce duplicate keys into the table!**
  ##
  ## Use `[]= proc<#[]=,Table[A,B],A,sinkB>`_ for inserting a new
  ## (key, value) pair in the table without introducing duplicates.
  addImpl(enlarge)

template tabMakeEmpty(i) = t.data[i].hcode = 0
template tabCellEmpty(i) = isEmpty(t.data[i].hcode)
template tabCellHash(i)  = t.data[i].hcode

proc del*[A, B](t: var Table[A, B], key: A) =
  ## Deletes `key` from hash table `t`. Does nothing if the key does not exist.
  ##
  ## .. warning:: If duplicate keys were added (via the now deprecated `add` proc),
  ##   this may need to be called multiple times.
  ##
  ## See also:
  ## * `pop proc<#pop,Table[A,B],A,B>`_
  ## * `clear proc<#clear,Table[A,B]>`_ to empty the whole table
  runnableExamples:
    var a = {'a': 5, 'b': 9, 'c': 13}.toTable
    a.del('a')
    doAssert a == {'b': 9, 'c': 13}.toTable
    a.del('z')
    doAssert a == {'b': 9, 'c': 13}.toTable

  delImpl(tabMakeEmpty, tabCellEmpty, tabCellHash)

proc pop*[A, B](t: var Table[A, B], key: A, val: var B): bool =
  ## Deletes the `key` from the table.
  ## Returns `true`, if the `key` existed, and sets `val` to the
  ## mapping of the key. Otherwise, returns `false`, and the `val` is
  ## unchanged.
  ##
  ## .. warning:: If duplicate keys were added (via the now deprecated `add` proc),
  ##   this may need to be called multiple times.
  ##
  ## See also:
  ## * `del proc<#del,Table[A,B],A>`_
  ## * `clear proc<#clear,Table[A,B]>`_ to empty the whole table
  runnableExamples:
    var
      a = {'a': 5, 'b': 9, 'c': 13}.toTable
      i: int
    doAssert a.pop('b', i) == true
    doAssert a == {'a': 5, 'c': 13}.toTable
    doAssert i == 9
    i = 0
    doAssert a.pop('z', i) == false
    doAssert a == {'a': 5, 'c': 13}.toTable
    doAssert i == 0

  var hc: Hash
  var index = rawGet(t, key, hc)
  result = index >= 0
  if result:
    val = move(t.data[index].val)
    delImplIdx(t, index, tabMakeEmpty, tabCellEmpty, tabCellHash)

proc take*[A, B](t: var Table[A, B], key: A, val: var B): bool {.inline.} =
  ## Alias for:
  ## * `pop proc<#pop,Table[A,B],A,B>`_
  pop(t, key, val)

proc clear*[A, B](t: var Table[A, B]) =
  ## Resets the table so that it is empty.
  ##
  ## See also:
  ## * `del proc<#del,Table[A,B],A>`_
  ## * `pop proc<#pop,Table[A,B],A,B>`_
  runnableExamples:
    var a = {'a': 5, 'b': 9, 'c': 13}.toTable
    doAssert len(a) == 3
    clear(a)
    doAssert len(a) == 0

  clearImpl()

proc `$`*[A, B](t: Table[A, B]): string =
  ## The `$` operator for hash tables. Used internally when calling `echo`
  ## on a table.
  dollarImpl()

proc `==`*[A, B](s, t: Table[A, B]): bool =
  ## The `==` operator for hash tables. Returns `true` if the content of both
  ## tables contains the same key-value pairs. Insert order does not matter.
  runnableExamples:
    let
      a = {'a': 5, 'b': 9, 'c': 13}.toTable
      b = {'b': 9, 'c': 13, 'a': 5}.toTable
    doAssert a == b

  equalsImpl(s, t)

proc indexBy*[A, B, C](collection: A, index: proc(x: B): C): Table[C, B] =
  ## Index the collection with the proc provided.
  # TODO: As soon as supported, change collection: A to collection: A[B]
  result = initTable[C, B]()
  for item in collection:
    result[index(item)] = item



template withValue*[A, B](t: var Table[A, B], key: A, value, body: untyped) =
  ## Retrieves the value at `t[key]`.
  ##
  ## `value` can be modified in the scope of the `withValue` call.
  runnableExamples:
    type
      User = object
        name: string
        uid: int

    var t = initTable[int, User]()
    let u = User(name: "Hello", uid: 99)
    t[1] = u

    t.withValue(1, value):
      # block is executed only if `key` in `t`
      value.name = "Nim"
      value.uid = 1314

    t.withValue(2, value):
      value.name = "No"
      value.uid = 521

    assert t[1].name == "Nim"
    assert t[1].uid == 1314

  mixin rawGet
  var hc: Hash
  var index = rawGet(t, key, hc)
  let hasKey = index >= 0
  if hasKey:
    var value {.inject.} = addr(t.data[index].val)
    body

template withValue*[A, B](t: var Table[A, B], key: A,
                          value, body1, body2: untyped) =
  ## Retrieves the value at `t[key]`.
  ##
  ## `value` can be modified in the scope of the `withValue` call.
  runnableExamples:
    type
      User = object
        name: string
        uid: int

    var t = initTable[int, User]()
    let u = User(name: "Hello", uid: 99)
    t[1] = u

    t.withValue(1, value):
      # block is executed only if `key` in `t`
      value.name = "Nim"
      value.uid = 1314

    t.withValue(521, value):
      doAssert false
    do:
      # block is executed when `key` not in `t`
      t[1314] = User(name: "exist", uid: 521)

    assert t[1].name == "Nim"
    assert t[1].uid == 1314
    assert t[1314].name == "exist"
    assert t[1314].uid == 521

  mixin rawGet
  var hc: Hash
  var index = rawGet(t, key, hc)
  let hasKey = index >= 0
  if hasKey:
    var value {.inject.} = addr(t.data[index].val)
    body1
  else:
    body2


iterator pairs*[A, B](t: Table[A, B]): (A, B) =
  ## Iterates over any `(key, value)` pair in the table `t`.
  ##
  ## See also:
  ## * `mpairs iterator<#mpairs.i,Table[A,B]>`_
  ## * `keys iterator<#keys.i,Table[A,B]>`_
  ## * `values iterator<#values.i,Table[A,B]>`_
  ##
  ## **Examples:**
  ##
  ## .. code-block::
  ##   let a = {
  ##     'o': [1, 5, 7, 9],
  ##     'e': [2, 4, 6, 8]
  ##     }.toTable
  ##
  ##   for k, v in a.pairs:
  ##     echo "key: ", k
  ##     echo "value: ", v
  ##
  ##   # key: e
  ##   # value: [2, 4, 6, 8]
  ##   # key: o
  ##   # value: [1, 5, 7, 9]
  let L = len(t)
  for h in 0 .. high(t.data):
    if isFilled(t.data[h].hcode):
      yield (t.data[h].key, t.data[h].val)
      assert(len(t) == L, "the length of the table changed while iterating over it")

iterator mpairs*[A, B](t: var Table[A, B]): (A, var B) =
  ## Iterates over any `(key, value)` pair in the table `t` (must be
  ## declared as `var`). The values can be modified.
  ##
  ## See also:
  ## * `pairs iterator<#pairs.i,Table[A,B]>`_
  ## * `mvalues iterator<#mvalues.i,Table[A,B]>`_
  runnableExamples:
    var a = {
      'o': @[1, 5, 7, 9],
      'e': @[2, 4, 6, 8]
      }.toTable
    for k, v in a.mpairs:
      v.add(v[0] + 10)
    doAssert a == {'e': @[2, 4, 6, 8, 12], 'o': @[1, 5, 7, 9, 11]}.toTable

  let L = len(t)
  for h in 0 .. high(t.data):
    if isFilled(t.data[h].hcode):
      yield (t.data[h].key, t.data[h].val)
      assert(len(t) == L, "the length of the table changed while iterating over it")

iterator keys*[A, B](t: Table[A, B]): lent A =
  ## Iterates over any key in the table `t`.
  ##
  ## See also:
  ## * `pairs iterator<#pairs.i,Table[A,B]>`_
  ## * `values iterator<#values.i,Table[A,B]>`_
  runnableExamples:
    var a = {
      'o': @[1, 5, 7, 9],
      'e': @[2, 4, 6, 8]
      }.toTable
    for k in a.keys:
      a[k].add(99)
    doAssert a == {'e': @[2, 4, 6, 8, 99], 'o': @[1, 5, 7, 9, 99]}.toTable

  let L = len(t)
  for h in 0 .. high(t.data):
    if isFilled(t.data[h].hcode):
      yield t.data[h].key
      assert(len(t) == L, "the length of the table changed while iterating over it")

iterator values*[A, B](t: Table[A, B]): lent B =
  ## Iterates over any value in the table `t`.
  ##
  ## See also:
  ## * `pairs iterator<#pairs.i,Table[A,B]>`_
  ## * `keys iterator<#keys.i,Table[A,B]>`_
  ## * `mvalues iterator<#mvalues.i,Table[A,B]>`_
  runnableExamples:
    let a = {
      'o': @[1, 5, 7, 9],
      'e': @[2, 4, 6, 8]
      }.toTable
    for v in a.values:
      doAssert v.len == 4

  let L = len(t)
  for h in 0 .. high(t.data):
    if isFilled(t.data[h].hcode):
      yield t.data[h].val
      assert(len(t) == L, "the length of the table changed while iterating over it")

iterator mvalues*[A, B](t: var Table[A, B]): var B =
  ## Iterates over any value in the table `t` (must be
  ## declared as `var`). The values can be modified.
  ##
  ## See also:
  ## * `mpairs iterator<#mpairs.i,Table[A,B]>`_
  ## * `values iterator<#values.i,Table[A,B]>`_
  runnableExamples:
    var a = {
      'o': @[1, 5, 7, 9],
      'e': @[2, 4, 6, 8]
      }.toTable
    for v in a.mvalues:
      v.add(99)
    doAssert a == {'e': @[2, 4, 6, 8, 99], 'o': @[1, 5, 7, 9, 99]}.toTable

  let L = len(t)
  for h in 0 .. high(t.data):
    if isFilled(t.data[h].hcode):
      yield t.data[h].val
      assert(len(t) == L, "the length of the table changed while iterating over it")

iterator allValues*[A, B](t: Table[A, B]; key: A): B {.deprecated:
    "Deprecated since v1.4; tables with duplicated keys are deprecated".} =
  ## Iterates over any value in the table `t` that belongs to the given `key`.
  ##
  ## Used if you have a table with duplicate keys (as a result of using
  ## `add proc<#add,Table[A,B],A,sinkB>`_).
  ##
  runnableExamples:
    import std/[sequtils, algorithm]

    var a = {'a': 3, 'b': 5}.toTable
    for i in 1..3: a.add('z', 10*i)
    doAssert toSeq(a.pairs).sorted == @[('a', 3), ('b', 5), ('z', 10), ('z', 20), ('z', 30)]
    doAssert sorted(toSeq(a.allValues('z'))) == @[10, 20, 30]
  var h: Hash = genHash(key) and high(t.data)
  let L = len(t)
  while isFilled(t.data[h].hcode):
    if t.data[h].key == key:
      yield t.data[h].val
      assert(len(t) == L, "the length of the table changed while iterating over it")
    h = nextTry(h, high(t.data))

