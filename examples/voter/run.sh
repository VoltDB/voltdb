#!/usr/bin/env bash

APPNAME="voter"
CLASSPATH="`ls -x ../../voltdb/voltdb-*.jar | tr '[:space:]' ':'``ls -x ../../lib/*.jar | tr '[:space:]' ':'`"
VOLTDB="../../bin/voltdb"
VOLTCOMPILER="../../bin/voltcompiler"
LOG4J="`pwd`/../../voltdb/log4j.xml"
LICENSE="../../voltdb/license.xml"
LEADER="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -target 1.6 -classpath $CLASSPATH -d obj \
        src/voter/*.java \
        src/voter/procedures/*.java
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
    java -classpath obj:$CLASSPATH:obj voter.AsyncBenchmark --help
}

function async-benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        voter.AsyncBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=120 \
        --servers=localhost:21212 \
        --contestants=6 \
        --maxvotes=2 \
        --ratelimit=100000 \
        --autotune=true \
        --latencytarget=6
}

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
function sync-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj voter.SyncBenchmark --help
}

function sync-benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        voter.SyncBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=120 \
        --servers=localhost:21212 \
        --contestants=6 \
        --maxvotes=2 \
        --threads=40
}

# JDBC benchmark sample
# Use this target for argument help
function jdbc-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj voter.JDBCBenchmark --help
}

function jdbc-benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        voter.JDBCBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --maxvotes=2 \
        --servers=localhost:21212 \
        --contestants=6 \
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
