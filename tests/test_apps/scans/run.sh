#!/usr/bin/env bash

APPNAME="scans"

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

CLASSPATH=$(ls -x "$VOLTDB_VOLTDB"/voltdb-*.jar | tr '[:space:]' ':')$(ls -x "$VOLTDB_LIB"/*.jar | egrep -v 'voltdb[a-z0-9.-]+\.jar' | tr '[:space:]' ':')
VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
CLIENTLOG4J="$VOLTDB_VOLTDB/../tests/log4j-allconsole.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput ${APPNAME}.jar voltdbroot log
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    mkdir -p obj
    # compile java source
    javac -classpath $CLASSPATH -d obj \
        src/${APPNAME}/*.java \
        src/${APPNAME}/procedures/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit 1; fi
    # build the jar file
    jar cf ${APPNAME}.jar -C obj ${APPNAME}
    if [ $? != 0 ]; then exit 2; fi
    # remove compiled .class files
    rm -rf obj
}

# compile the jar file, if it doesn't exist
function jars-ifneeded() {
    if [ ! -e ${APPNAME}.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    jars-ifneeded
    # truncate the voltdb log
    [[ -d voltdbroot/log && -w voltdbroot/log ]] && > voltdbroot/log/volt.log
    # run the server
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command line: "
    echo
    echo "${VOLTDB} init -C deployment.xml -j ${APPNAME}.jar -s ddl.sql --force"
    echo "${VOLTDB} start -l ${LICENSE} -H ${HOST}"
    echo
    ${VOLTDB} init -C deployment.xml -j ${APPNAME}.jar -s ddl.sql --force
    ${VOLTDB} start -l ${LICENSE} -H ${HOST}
}

# run the client that drives the example
function client() {
    benchmark
}

function benchmark() {
    srccompile
    java -classpath ${CLASSPATH}:${APPNAME}.jar -Dlog4j.configuration=file://$CLIENTLOG4J \
        scans.ScanBenchmark \
        --runs=20 \
        --rows=25000000 \
        --servers=localhost:21212
}

function help() {
    echo "Usage: ./run.sh {clean|jars[-ifneeded]|server|client|benchmark}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
