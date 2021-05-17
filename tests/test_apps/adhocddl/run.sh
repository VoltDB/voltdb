#!/usr/bin/env bash

#set -o nounset #exit if an unset variable is used
set -o errexit #exit on any single command fail

export VOLTDB_HEAPMAX=8192
APPNAME="adhocddl"

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(pwd)/../../../bin"
fi
# move voltdb commands into path for this script
PATH=$VOLTDB_BIN:$PATH

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
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot ddl.sql src/adhocddl/*.class
}

# run the voltdb server locally
function server() {
    voltdb init -C deployment.xml
    voltdb start -H $HOST
}

# run the client that drives the example
function client() {
    javac -classpath $CLASSPATH src/adhocddl/*.java

    java -classpath src:$CLASSPATH adhocddl.AdHocDDLBenchmark \
        --servers=localhost \
        --numRounds=5 \
        --numCols=20 \
        --numTables=40 \
        --idxPercent=0.1 \
        --numProcedures=2 \
        --statsfile=results.txt
}

function help() {
    echo "Usage: ./run.sh {clean|server|client}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
