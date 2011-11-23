#!/usr/bin/env bash

APPNAME="eng1969"
CLASSPATH="`ls -x ../../../voltdb/voltdb-*.jar | tr '[:space:]' ':'``ls -x ../../../lib/*.jar | tr '[:space:]' ':'`"
VOLTDB="../../../bin/voltdb"
VOLTCOMPILER="../../../bin/voltcompiler"
LICENSE="../../../voltdb/license.xml"
LEADER="localhost"
EXPORTDATA="exportdata"
CLIENTLOG="clientlog"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot plannerlog.txt voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj \
        src/$APPNAME/*.java \
        src/$APPNAME/procedures/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
    $VOLTCOMPILER obj project.xml $APPNAME.jar
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
        license $LICENSE leader $LEADER
}

# run the client that drives the example
function client() {
    async-benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj eng1969.AsyncBenchmark --help
}

function async-benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj eng1969.AsyncBenchmark \
        --display-interval=5 \
        --duration=120 \
        --servers=localhost \
        --port=21212 \
        --procedure=UpdateKey \
        --pool-size=100000 \
        --wait=0 \
        --rate-limit=25000 \
        --auto-tune=true \
        --latency-target=12.0 \
        --run-loader=false
}

function async-export() {
    srccompile
    rm -rf $CLIENTLOG/*
    mkdir $CLIENTLOG
    java -classpath obj:$CLASSPATH:obj eng1969.AsyncExportClient \
        --display-interval=5 \
        --duration=6000 \
        --servers=localhost \
        --port=21212 \
        --pool-size=100000 \
        --rate-limit=10000 \
        --auto-tune=false \
        --latency-target=10.0
}

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
function sync-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj eng1969.SyncBenchmark --help
}

function sync-benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj eng1969.SyncBenchmark \
        --threads=40 \
        --display-interval=5 \
        --duration=120 \
        --servers=localhost \
        --port=21212 \
        --procedure=JiggleSinglePartition \
        --pool-size=100000 \
        --wait=0
}

# JDBC benchmark sample
# Use this target for argument help
function jdbc-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj eng1969.JDBCBenchmark --help
}

function jdbc-benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj eng1969.JDBCBenchmark \
        --threads=40 \
        --display-interval=5 \
        --duration=120 \
        --servers=localhost \
        --port=21212 \
        --procedure=JiggleSinglePartition \
        --pool-size=100000 \
        --wait=0
}

function export() {
    rm -rf $EXPORTDATA/*
    mkdir $EXPORTDATA
    java -classpath obj:$CLASSPATH:obj org.voltdb.exportclient.ExportToFileClient \
        --connect client \
        --servers localhost \
        --type csv \
        --outdir ./$EXPORTDATA \
        --nonce export \
        --period 1
}

function exportverify() {
    java -classpath obj:$CLASSPATH:obj eng1969.ExportVerifier \
        4 \
        $EXPORTDATA \
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
