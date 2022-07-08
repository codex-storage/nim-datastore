--threads:on
--tlsEmulation:off

when (NimMajor, NimMinor) == (1, 2):
  switch("hint", "Processing:off")
  switch("hint", "XDeclaredButNotUsed:off")
  switch("warning", "ObservableStores:off")

when (NimMajor, NimMinor) > (1, 2):
  switch("hint", "XCannotRaiseY:off")
