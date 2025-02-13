--threads:on
--tlsEmulation:off
--styleCheck:usages
--styleCheck:error

when (NimMajor, NimMinor) == (1, 2):
  switch("hint", "Processing:off")
  switch("hint", "XDeclaredButNotUsed:off")
  switch("warning", "ObservableStores:off")

when (NimMajor, NimMinor) > (1, 2):
  switch("hint", "XCannotRaiseY:off")
# begin Nimble config (version 2)
when withDir(thisDir(), system.fileExists("nimble.paths")):
  include "nimble.paths"
# end Nimble config

when (NimMajor, NimMinor) >= (2, 0):
  --mm:refc

