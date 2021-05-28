#!/usr/bin/env bash

APPNAME="txnid"

# find voltdb binaries (customized from examples to be one level deeper)
if [ -e ../../../bin/voltdb ]; then
    # assume this is the examples folder for a kit
    VOLTDB_BIN="$(dirname $(dirname $(dirname $(pwd))))/bin"
elif [ -n "$(which voltdb 2> /dev/null)" ]; then
    # assume we're using voltdb from the path
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    echo "Unable to find VoltDB installation."
    echo "Please add VoltDB's bin directory to your path."
    exit -1
fi

# call script to set up paths, including
# java classpaths and binary paths
source $VOLTDB_BIN/voltenv

# leader host for startup purposes only
# (once running, all nodes are the same -- no leaders)
STARTUPLEADERHOST="localhost"
# list of cluster nodes separated by commas in host:[port] format
SERVERS="localhost"

# remove build artifacts
function clean() {
    rm -rf voltdbroot log $APPNAME.jar \
        src/txnIdSelfCheck/*.class src/txnIdSelfCheck/procedures/*.class
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -classpath $APPCLASSPATH \
        src/txnIdSelfCheck/procedures/*.java src/txnIdSelfCheck/*.java
    # build procedure and client jars
    jar cf $APPNAME-procs.jar -C src txnIdSelfCheck/procedures
    jar cf $APPNAME-client.jar -C src txnIdSelfCheck
    # remove compiled .class files
    rm -rf src/txnIdSelfCheck/procedures/*.class src/txnIdSelfCheck/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e $APPNAME-procs.jar ] || [ ! -e $APPNAME-client.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    # note: "init --force" will delete any existing data
    voltdb init --force
    voltdb start -H $STARTUPLEADERHOST
}

# load schema and procedures
function init() {
    jars-ifneeded
    sqlcmd < ddl.sql
}

# run the client that drives the example
function client() {
    async-benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    jars-ifneeded
    java -classpath obj:$CLASSPATH:obj txnIdSelfCheck.AsyncBenchmark --help
}

function async-benchmark() {
    jars-ifneeded
    java -ea -classpath $APPNAME-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$CLIENTLOG4J \
        txnIdSelfCheck.AsyncBenchmark \
        --displayinterval=1 \
        --duration=120 \
        --servers=localhost \
        --multisingleratio=0.001 \
        --windowsize=100000 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --entropy=127 \
        --usecompression=false \
        --ratelimit=20000
}

function help() {
    echo "Usage: ./run.sh {clean|jars|server|init|async-benchmark|aysnc-benchmark-help}"
}

# Run the targets pass on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then server; exit; fi
for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
