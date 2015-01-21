#!/usr/bin/env bash

APPNAME="helloworld"

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

APPCLASSPATH=$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot voltdbroot
}

function jars() {
    # compile java source
    javac -target 1.7 -source 1.7 -classpath $APPCLASSPATH *.java
    # build procedure and client jars
    jar cf $APPNAME.jar *.class
    # remove compiled .class files
    rm -rf *.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e $APPNAME.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    # run the server
    $VOLTDB create -d deployment.xml -l $LICENSE -H localhost
}

# load schema and procedures
function init() {
    jars-ifneeded
    $VOLTDB_BIN/sqlcmd < helloworld.sql
}

# run the client that drives the example
function client() {
    jars-ifneeded
    java -classpath $APPCLASSPATH:$APPNAME.jar Client
}

function help() {
    echo "Usage: ./run.sh {clean|init|client|server}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
