#!/usr/bin/env bash

APPNAME="overhead"
CLASSPATH="`ls -x ../../../voltdb/voltdb-*.jar | tr '[:space:]' ':'``ls -x ../../../lib/*.jar | tr '[:space:]' ':'`"
VOLTDB="../../../bin/voltdb"
VOLTCOMPILER="../../../bin/voltcompiler"
LOG4J="`pwd`/../../../voltdb/log4j.xml"
LICENSE="../../../voltdb/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj \
        src/overhead/*.java \
        src/overhead/procedures/*.java
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

# run the client that drives the example
function client() {
    async-benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj overhead.AsyncBenchmark --help
}

function async-benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        overhead.AsyncBenchmark \
        --displayinterval=5 \
        --duration=60 \
        --operation=BinaryPayloadRW \
        --servers=localhost \
        --port=21212 \
        --resultsize=0 \
        --paramsize=0 \
        --ratelimit=900000
}



function help() {
    echo "Usage: ./run.sh {clean|catalog|server|async-benchmark|aysnc-benchmark-help|...}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
