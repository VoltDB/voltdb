#!/usr/bin/env bash

APPNAME="genqa"
APPNAME2="genqa2"
CLASSPATH="`ls -x ../../../voltdb/voltdb-*.jar | tr '[:space:]' ':'``ls -x ../../../lib/*.jar | tr '[:space:]' ':'`"
VOLTDB="../../../bin/voltdb"
VOLTCOMPILER="../../../bin/voltcompiler"
LICENSE="../../../voltdb/license.xml"
HOST="localhost"
EXPORTDATA="exportdata"
CLIENTLOG="clientlog"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar $APPNAME2.jar voltdbroot voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj \
        src/$APPNAME/*.java \
        src/$APPNAME/procedures/*.java
    javac -classpath $CLASSPATH -d obj \
        src/$APPNAME2/procedures/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
    $VOLTCOMPILER obj project.xml $APPNAME.jar
    $VOLTCOMPILER obj project2.xml $APPNAME2.jar
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
        --pool-size=100000 \
        --wait=0 \
        --ratelimit=100000 \
        --autotune=true \
        --latencytarget=10
}

function async-export() {
    srccompile
    rm -rf $CLIENTLOG/*
    mkdir $CLIENTLOG
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
        --pool-size=100000 \
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
    java -classpath obj:$CLASSPATH:obj org.voltdb.exportclient.ExportToFileClient \
        --connect client \
        --servers localhost \
        --type csv \
        --outdir ./$EXPORTDATA \
        --nonce export \
        --period 1
}

function export-tosqoop() {
    echo "Running sqoop export process"
    #rm -rf $EXPORTDATA
    #Change these if sqoop or hadoop are installed elsewhere
    export SQOOP_HOME="/usr/lib/sqoop"
    export HADOOP_HOME="/usr/lib/hadoop"
    H_PATH="$HADOOP_HOME/*:$HADOOP_HOME/conf:$HADOOP_HOME/lib/*"
    S_PATH="$SQOOP_HOME/*:$SQOOP_HOME/lib/*"
    export CLASSPATH="$CLASSPATH:$H_PATH:$S_PATH"
    java org.voltdb.hadoop.VoltDBSqoopExportClient \
       --connect client \
       --servers localhost \
       --verbose \
       --period 3 \
       --target-dir /tmp/sqoop-export \
       --nonce ExportData
}


function exportverify() {
    java -classpath obj:$CLASSPATH:obj genqa.ExportVerifier \
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
