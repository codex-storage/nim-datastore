mode = ScriptMode.Verbose

packageName   = "datastore"
version       = "0.0.1"
author        = "Status Research & Development GmbH"
description   = "Simple, unified API for multiple data stores"
license       = "Apache License 2.0 or MIT"

requires "nim >= 1.2.0",
         "questionable >= 0.10.3 & < 0.11.0",
         "sqlite3_abi",
         "stew",
         "unittest2",
         "upraises >= 0.1.0 & < 0.2.0"

task coverage, "generates code coverage report":
  var (output, exitCode) = gorgeEx("which lcov")
  if exitCode != 0:
    echo ""
    echo "  ************************** ⛔️ ERROR ⛔️ **************************"
    echo "  **                                                             **"
    echo "  **   ERROR: lcov not found, it must be installed to run code   **"
    echo "  **   coverage locally                                          **"
    echo "  **                                                             **"
    echo "  *****************************************************************"
    echo ""
    quit 1

  (output, exitCode) = gorgeEx("gcov --version")
  if output.contains("Apple LLVM"):
    echo ""
    echo "  ************************* ⚠️  WARNING ⚠️  *************************"
    echo "  **                                                             **"
    echo "  **   WARNING: Using Apple's llvm-cov in place of gcov, which   **"
    echo "  **   emulates an old version of gcov (4.2.0) and therefore     **"
    echo "  **   coverage results will differ than those on CI (which      **"
    echo "  **   uses a much newer version of gcov).                       **"
    echo "  **                                                             **"
    echo "  *****************************************************************"
    echo ""

  exec("nimble --verbose test --verbosity:0 --hints:off --lineDir:on --nimcache:nimcache --passC:-fprofile-arcs --passC:-ftest-coverage --passL:-fprofile-arcs --passL:-ftest-coverage")
  exec("cd nimcache; rm *.c; cd ..")
  mkDir("coverage")
  exec("lcov --capture --directory nimcache --output-file coverage/coverage.info")
  exec("$(which bash) -c 'shopt -s globstar; ls $(pwd)/datastore/{*,**/*}.nim'")
  exec("$(which bash) -c 'shopt -s globstar; lcov --extract coverage/coverage.info  $(pwd)/datastore/{*,**/*}.nim --output-file coverage/coverage.f.info'")
  echo "Generating HTML coverage report"
  exec("genhtml coverage/coverage.f.info --output-directory coverage/report")
  echo "Opening HTML coverage report in browser..."
  exec("open coverage/report/index.html")
