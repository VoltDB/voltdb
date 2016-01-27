#!/usr/bin/env bash

APPNAME="log4jsocketimporter"

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
elif [ -d "$VOLTDB_BIN/../voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib"
    VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"
else
    VOLTDB_LIB="`pwd`/../../../lib"
    VOLTDB_VOLTDB="`pwd`/../../../voltdb"
fi

CLASSPATH=$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )


VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="`pwd`/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"

# remove build artifacts
function clean() {
    rm -rf classes debugoutput $APPNAME-client.jar voltdbroot log felix-cache
}

# compile the source code for client
function buildclient() {
    mkdir -p classes
    javac -classpath $CLASSPATH -d classes \
        src/$APPNAME/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi

    jar cf $APPNAME-client.jar -C classes $APPNAME
    # stop on jar error
    if [ $? != 0 ]; then exit; fi
}

# Initialize the application by loading the procedures and table defns
function init() {
    buildclient
    sqlcmd < ddl.sql
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # run the server
    $VOLTDB create -d deployment.xml -l $LICENSE -H localhost
}

function testclient() {
    buildclient
    java -classpath $CLASSPATH:$APPNAME-client.jar -Dlog4j.configuration=file://$LOG4J \
        $APPNAME.LogTestClient localhost
}

function help() {
    echo "Usage: ./run.sh {server|init|testclient|clean}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
