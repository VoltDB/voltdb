#!/usr/bin/env bash

APPNAME="voltkv"

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
VOLTCOMPILER="$VOLTDB_BIN/voltcompiler"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj \
        src/voltkvqa/*.java \
        src/voltkvqa/procedures/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
    $VOLTCOMPILER obj project.xml $APPNAME.jar
    # stop if compilation fails
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

function exportserver() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment deployment_export.xml \
        license $LICENSE host $HOST enableiv2
}

# run the client that drives the example
function client() {
    async-benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj voltkvqa.AsyncBenchmark --help
}

#        --servers=volt3d,volt3e,volt3f \
function async-benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        voltkvqa.AsyncBenchmark \
        --displayinterval=5 \
        --duration=60 \
        --servers=localhost \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.9 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --entropy=127 \
        --usecompression=false \
        --ratelimit=100000 \
        --autotune=true \
        --multisingleratio=0.1 \
        --recover=false \
        --latencytarget=10
}

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
function sync-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj voltkvqa.SyncBenchmark --help
}

function sync-benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        voltkvqa.SyncBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --usecompression=false \
        --threads=40
}

# JDBC benchmark sample
# Use this target for argument help
function jdbc-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj voltkvqa.JDBCBenchmark --help
}

function jdbc-benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        voltkvqa.JDBCBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost:21212 \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --usecompression=false \
        --threads=40
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|server|async-benchmark|aysnc-benchmark-help|...}"
    echo "       {...|sync-benchmark|sync-benchmark-help|jdbc-benchmark|jdbc-benchmark-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
