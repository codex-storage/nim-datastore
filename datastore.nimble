mode = ScriptMode.Verbose

packageName   = "datastore"
version       = "0.0.1"
author        = "Status Research & Development GmbH"
description   = "Simple, unified API for multiple data stores"
license       = "Apache License 2.0 or MIT"

requires "nim >= 1.6.14"
requires "asynctest#fe1a34caf572b05f8bdba3b650f1871af9fce31e"
requires "chronos#0277b65be2c7a365ac13df002fba6e172be55537"
requires "questionable >= 0.10.3 & < 0.11.0"
requires "sqlite3_abi"
requires "stew#7afe7e3c070758cac1f628e4330109f3ef6fc853"
requires "unittest2#b178f47527074964f76c395ad0dfc81cf118f379"
requires "pretty"
requires "threading"
requires "taskpools"
requires "upraises#ff4f8108e44fba9b35cac535ab63d3927e8fd3c2"

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
