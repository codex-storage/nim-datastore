--threads:on
--tlsEmulation:off
--styleCheck:usages
--styleCheck:error

# --d:"datastoreUseAsync=false"

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
