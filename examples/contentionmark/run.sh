#!/usr/bin/env bash

#set -o nounset #exit if an unset variable is used
set -o errexit #exit on any single command fail

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(dirname $(dirname $(pwd)))/bin"
    echo "The VoltDB scripts are not in your PATH."
    echo "For ease of use, add the VoltDB bin directory: "
    echo
    echo $VOLTDB_BIN
    echo
    echo "to your PATH."
    echo
fi
# move voltdb commands into path for this script
PATH=$VOLTDB_BIN:$PATH

# installation layout has all libraries in $VOLTDB_ROOT/lib/voltdb
if [ -d "$VOLTDB_BIN/../lib/voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib/voltdb"
    VOLTDB_VOLTDB="$VOLTDB_LIB"
# distribution layout has libraries in separate lib and voltdb directories
else
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib"
    VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"
fi

APPCLASSPATH=$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
CLIENTCLASSPATH=contentionmark-client.jar:$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdbclient-*.jar; \
} 2> /dev/null | paste -sd ':' - )
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
HOST="localhost"

# remove binaries, logs, runtime artifacts, etc... but keep the jars
function clean() {
    rm -rf debugoutput voltdbroot log catalog-report.html \
         statement-plans \
         *.class \
         contentionmark-client.jar
}

function cleanall() {
    clean
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -classpath $CLIENTCLASSPATH ContentionMark.java
    # build procedure and client jars
    jar cf contentionmark-client.jar *.class
    # remove compiled .class files
    rm -rf *.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e contentionmark-client.jar ]; then
        jars;
    fi
}

# Start DB, load schema, procedures, and static data
function init() {
    jars-ifneeded
    sqlcmd < ddl.sql
}

function server() {
    voltdb create -H $HOST
}

# run the client that drives the example
function client() {
    jars-ifneeded
    java -classpath $CLIENTCLASSPATH ContentionMark \
        --duration=60 \
    	--tuples=1 \
    	--servers=localhost
}

function help() {
    echo "Usage: ./run.sh {server|clean|init|client}"
}

# Run the targets pass on the command line
# If no first arg, run demo
if [ $# -eq 0 ]; then server; exit; fi
for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
