#!/usr/bin/env bash

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(pwd)/../../../bin"
fi
# installation layout has all libraries in $VOLTDB_ROOT/lib/voltdb
if [ -d "$VOLTDB_BIN/../lib/voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib/voltdb"
    VOLTDB_VOLTDB="$VOLTDB_LIB"
# distribution layout has libraries in separate lib and voltdb directories
else
    VOLTDB_LIB="`pwd`/../../../lib"
    VOLTDB_VOLTDB="`pwd`/../../../voltdb"
fi

CLASSPATH=$(ls -x "$VOLTDB_VOLTDB"/voltdb-*.jar | tr '[:space:]' ':')$(ls -x "$VOLTDB_LIB"/*.jar | egrep -v 'voltdb[a-z0-9.-]+\.jar' | tr '[:space:]' ':')
VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -target 1.6 -source 1.6 -classpath $CLASSPATH -d obj *.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
    $VOLTDB compile --classpath obj -o $APPNAME.jar helloworld.sql
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment deployment.xml \
        license $LICENSE host localhost
}

# run the client that drives the example
function client() {
    srccompile
    java -classpath obj:$CLASSPATH Client $SERVERLIST
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|client|server}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 2 ]; then help; exit; fi
if [ $# = 2 ]; then SERVERLIST="$2"; fi
if [ $# -gt 0 ]; then $1; else server; fi
