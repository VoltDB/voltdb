#!/usr/bin/env bash
#
# Compiles the UserDefinedTestFunctions class, which contains multiple UDFs
# (user-defined (Java) functions), and creates two different jar files
# containing it, which are used to test UDFs. One of the jar files is an
# "alternative" version, with USE_ALTERNATIVE_VERSION set to 'true'; the other
# is a "normal" version, with USE_ALTERNATIVE_VERSION set to 'false'.

help() {
  echo 'Usage: ./build_udf_jar.sh [--build=BUILD]'
  echo '  Build the UDF (user-defined function) jars, which are used to test'
  echo '  UDFs, e.g., by sqlcmd test.'
  echo '  Use BUILD for the build type. The default build type is release.'
}

# set the build type
BUILD=release
while [ -n "$1" ] ; do
  case "$1" in
  --build=*)
    BUILD=$(echo "$1" | sed 's/--build=//')
    shift
    ;;
  -h|--help)
    help
    exit 100
    ;;
  *)
    echo "$0: Unknown command line argument $1"
    help
    exit 100
    ;;
  esac
done

# modify UserDefinedTestFunctions.java to use its "alternative" version of (some of) the UDF's
sed -i '' -e 's/USE_ALTERNATIVE_VERSION = false/USE_ALTERNATIVE_VERSION = true/' org/voltdb_testfuncs/UserDefinedTestFunctions.java

# compile the "alternative" version of the class needed for the jar file
javac -classpath ../../obj/$BUILD/prod org/voltdb_testfuncs/UserDefinedTestFunctions.java

# build the "alternative" version of the jar file
jar cf testfuncs_alternative.jar org/voltdb_testfuncs/UserDefinedTestFunctions.class \
                                 org/voltdb_testfuncs/UserDefinedTestFunctions\$UserDefinedTestException.class \
                                 org/voltdb_testfuncs/UserDefinedTestFunctions\$UDF_TEST.class

# modify UserDefinedTestFunctions.java back to using its "normal" version of all UDF's
sed -i '' -e 's/USE_ALTERNATIVE_VERSION = true/USE_ALTERNATIVE_VERSION = false/' org/voltdb_testfuncs/UserDefinedTestFunctions.java

# compile the "normal" version of the class needed for the jar file
javac -classpath ../../obj/$BUILD/prod org/voltdb_testfuncs/UserDefinedTestFunctions.java

# build the "normal" version of the jar file
jar cf testfuncs.jar org/voltdb_testfuncs/UserDefinedTestFunctions.class \
                     org/voltdb_testfuncs/UserDefinedTestFunctions\$UserDefinedTestException.class \
                     org/voltdb_testfuncs/UserDefinedTestFunctions\$UDF_TEST.class

# remove compiled .class files
rm -rf org/voltdb_testfuncs/*.class
