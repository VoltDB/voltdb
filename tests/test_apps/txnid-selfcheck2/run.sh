#!/usr/bin/env bash

APPNAME="txnid"

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
if [ -f "$VOLTDB_VOLTDB/../tests/log4j-allconsole.xml" ]; then
    CLIENTLOG4J="$VOLTDB_VOLTDB/../tests/log4j-allconsole.xml"
elif [ -f  $PWD/../../log4j-allconsole.xml ]; then
    CLIENTLOG4J="$PWD/../../log4j-allconsole.xml"
fi
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj log build debugoutput $APPNAME.jar $APPNAME-alt.jar $APPNAME-noexport.jar voltdbroot
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    ant clean
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    ant
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e txnid.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    jars-ifneeded
    # run the server
    $VOLTDB init -C deployment.xml
    $VOLTDB start -l $LICENSE -H $HOST
}

# run the client that drives the example
function client() {
    async-benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    jars-ifneeded
    java -classpath $CLASSPATH:txnid.jar txnIdSelfCheck.Benchmark --help
}

function async-benchmark() {
    jars-ifneeded
    java -ea -classpath txnid.jar:$CLASSPATH: -Dlog4j.configuration=file://$CLIENTLOG4J \
        txnIdSelfCheck.Benchmark $ARGS \
        --displayinterval=1 \
        --duration=100 \
        --servers=localhost \
        --threads=20 \
        --threadoffset=0 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --entropy=127 \
        --fillerrowsize=10240 \
        --replfillerrowmb=32 \
        --partfillerrowmb=128 \
        --progresstimeout=20 \
        --usecompression=false \
        --allowinprocadhoc=false
        # --disabledthreads=ddlt,partBiglt,replBiglt,partCappedlt,replCappedlt,replLoadlt,partLoadlt,adHocMayhemThread,idpt,partTrunclt,replTrunclt
#ddlt,clients,partBiglt,replBiglt,partCappedlt,replCappedlt,replLoadlt,partLoadlt,adHocMayhemThread,idpt,readThread,partTrunclt,replTrunclt
}

function init() {
    jars-ifneeded
    sqlcmd < src/txnIdSelfCheck/ddl-nocat.sql
}

function help() {
    echo "Usage: ./run.sh {clean|jars|init|server|async-benchmark|async-benchmark-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
# If more than one arg, pass the rest to the client (async-benchmark)
ARGS=""
if [ $# -gt 1 ]; then
    if [ $1 = "client" ] || [ $1 = "async-benchmark" ]; then
        ARGS="${@:2}";
    else
        help; exit;
    fi;
fi
if [ $# -ge 1 ]; then $1; else server; fi
