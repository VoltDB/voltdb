#!/usr/bin/env bash

APPNAME="schemachange"

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
CLIENTLOG4J="$VOLTDB_VOLTDB/../tests/log4j-allconsole.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj \
        src/schemachange/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit 1; fi
}

# build an application catalog
function catalog() {
    srccompile
    $VOLTDB compile --classpath obj -o $APPNAME.jar ddl.sql
    # stop if compilation fails
    if [ $? != 0 ]; then exit 1; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create -d deployment.xml -l $LICENSE -H $HOST $APPNAME.jar
}

# run the client that drives the example
function client() {
    srccompile
    java -ea -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$CLIENTLOG4J \
        schemachange.SchemaChangeClient \
        --servers=localhost \
        --targetrowcount=100000 \
        --duration=1800
}

# quick client run
function quick() {
    srccompile
    java -ea -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$CLIENTLOG4J \
        schemachange.SchemaChangeClient \
        --servers=localhost \
        --targetrowcount=1000 \
        --duration=60
}

# quick smoke test, including forced failures
function smoke() {
    srccompile
    java -ea -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$CLIENTLOG4J \
        schemachange.SchemaChangeClient \
        --servers=localhost \
        --targetrowcount=1000 \
        --duration=60 \
        --retryForcedPercent=50 \
        --retryLimit=10 \
        --retrySleep=2
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|server|client}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit 1; fi
if [ $# = 1 ]; then $1; else server; fi
