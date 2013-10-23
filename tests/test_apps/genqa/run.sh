#!/usr/bin/env bash

APPNAME="genqa"
APPNAME2="genqa2"

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

CLASSPATH=$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
VOLTDB="$VOLTDB_BIN/voltdb"
VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"
EXPORTDATA="exportdata"
EXPORTDATAREMOTE="localhost:${PWD}/${EXPORTDATA}"
CLIENTLOG="clientlog"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar $APPNAME2.jar voltdbroot voltdbroot
    rm -f $VOLTDB_LIB/extension/customexport.jar
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj \
        src/$APPNAME/*.java \
        src/$APPNAME/procedures/*.java
    javac -classpath $CLASSPATH -d obj \
        src/$APPNAME2/procedures/*.java
    javac -classpath $CLASSPATH -d obj \
        src/customexport/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
    $VOLTDB compile --classpath obj -o $APPNAME.jar -p project.xml
    $VOLTDB compile --classpath obj -o $APPNAME2.jar -p project2.xml
    # stop if compilation fails
    rm -rf $EXPORTDATA
    mkdir $EXPORTDATA
    rm -rf $CLIENTLOG
    mkdir $CLIENTLOG
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment deployment.xml \
        license $LICENSE host $HOST
}

# run the voltdb server locally
function server-legacy() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment deployment_legacy.xml \
        license $LICENSE host $HOST
}

function server-custom() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # Get custom class in jar
    cd obj
    jar cvf ../customexport.jar customexport/*
    cd ..
    cp customexport.jar $VOLTDB_LIB/extension/customexport.jar
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment deployment_custom.xml \
        license $LICENSE host $HOST
}

# run the voltdb server locally
function server1() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment deployment_multinode.xml \
        license $LICENSE host $HOST:3021 internalport 3024 enableiv2
}

function server2() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment deployment_multinode.xml \
        license $LICENSE host $HOST:3021 internalport 3022 adminport 21215 port 21216 zkport 2182 enableiv2
}

function server3() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment deployment_multinode.xml \
        license $LICENSE host $HOST:3021 internalport 3023 adminport 21213 port 21214 zkport 2183 enableiv2
}


# run the client that drives the example
function client() {
    async-benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj genqa.AsyncBenchmark --help
}

function async-benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj genqa.AsyncBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --port=21212 \
        --procedure=JiggleSinglePartition \
        --poolsize=100000 \
        --wait=0 \
        --ratelimit=100000 \
        --autotune=true \
        --latencytarget=10
}

function async-export() {
    srccompile
    rm -rf $CLIENTLOG/*
    mkdir $CLIENTLOG
    echo file:/${PWD}/../../log4j-allconsole.xml
    java -classpath obj:$CLASSPATH:obj genqa.AsyncExportClient \
        --displayinterval=5 \
        --duration=900 \
        --servers=localhost \
        --port=21212 \
        --poolsize=100000 \
        --ratelimit=10000 \
        --autotune=false \
        --catalogswap=false \
        --latencytarget=10
}

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
function sync-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj genqa.SyncBenchmark --help
}

function sync-benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj genqa.SyncBenchmark \
        --threads=40 \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --port=21212 \
        --procedure=JiggleSinglePartition \
        --poolsize=100000 \
        --wait=0
}

# JDBC benchmark sample
# Use this target for argument help
function jdbc-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj genqa.JDBCBenchmark --help
}

function jdbc-benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj genqa.JDBCBenchmark \
        --threads=40 \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --port=21212 \
        --procedure=JiggleSinglePartition \
        --poolsize=100000 \
        --wait=0
}

function export-tofile() {
    rm -rf $EXPORTDATA/*
    mkdir $EXPORTDATA
    java -Dlog4j.configuration=file:${PWD}/../../log4j-allconsole.xml \
         -classpath obj:$CLASSPATH:obj org.voltdb.exportclient.ExportToFileClient \
        --connect client \
        --servers localhost \
        --type csv \
        --outdir ./$EXPORTDATA \
        --nonce export \
        --period 1
}

function export-verify() {
    java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Xmx512m -classpath obj:$CLASSPATH:obj genqa.ExportVerifier \
        4 \
        $EXPORTDATA \
        $CLIENTLOG
}

function export-on-server-verify() {
    java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Xmx512m -classpath obj:$CLASSPATH:obj genqa.ExportOnServerVerifier \
        $EXPORTDATAREMOTE \
        4 \
        $CLIENTLOG
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|server|async-benchmark|async-benchmark-help|...}"
    echo "       {...|sync-benchmark|sync-benchmark-help|jdbc-benchmark|jdbc-benchmark-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
