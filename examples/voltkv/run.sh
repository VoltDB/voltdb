#!/usr/bin/env bash

# find voltdb binaries
if [ -e ../../bin/voltdb ]; then
    # assume this is the examples folder for a kit
    VOLTDB_BIN="$(dirname $(dirname $(pwd)))/bin"
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

# remove binaries, logs, runtime artifacts, etc... but keep the client jar
function clean() {
    rm -rf client/voltkv/*.class voltdbroot log
}

# remove everything from "clean" as well as the jarfile
function cleanall() {
    clean
    rm -rf voltkv-client.jar
}

# compile the source code for the client into a jarfile
function jars() {
    # compile java source
    javac -classpath $CLIENTCLASSPATH client/voltkv/*.java
    # build client jar
    jar cf voltkv-client.jar -C client voltkv
    # remove compiled .class files
    rm -rf client/voltkv/*.class
}

# compile the client jarfile if it doesn't exist
function jars-ifneeded() {
    if [ ! -e voltkv-client.jar ]; then
        jars;
    fi
}

# Init to directory voltdbroot
function voltinit-ifneeded() {
    voltdb init --force
}

# run the voltdb server locally
function server() {
    voltinit-ifneeded
    voltdb start -H $STARTUPLEADERHOST
}

# load schema and procedures
function init() {
    jars-ifneeded
    sqlcmd --servers=$SERVERS < ddl.sql
}

# run the client that drives the example
function client() {
    async-benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    jars-ifneeded
    java -classpath voltkv-client.jar:$CLIENTCLASSPATH voltkv.AsyncBenchmark --help
}

# latencyreport: default is OFF
# ratelimit: must be a reasonable value if lantencyreport is ON
# Disable the comments (and add a preceding slash) to get latency report
function async-benchmark() {
    jars-ifneeded
    java -classpath voltkv-client.jar:$CLIENTCLASSPATH voltkv.AsyncBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=$SERVERS \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --entropy=127 \
        --usecompression=false 
#        --multisingleratio=0.0
#        --latencyreport=true \
#        --ratelimit=100000 \
}

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
function sync-benchmark-help() {
    jars-ifneeded
    java -classpath voltkv-client.jar:$CLIENTCLASSPATH voltkv.SyncBenchmark --help
}

function sync-benchmark() {
    jars-ifneeded
    java -classpath voltkv-client.jar:$CLIENTCLASSPATH voltkv.SyncBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=$SERVERS \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --usecompression=false \
        --threads=40
#        --multisingleratio=0.0
}

# JDBC benchmark sample
# Use this target for argument help
function jdbc-benchmark-help() {
    jars-ifneeded
    java -classpath voltkv-client.jar:$CLIENTCLASSPATH voltkv.JDBCBenchmark --help
}

function jdbc-benchmark() {
    jars-ifneeded
    java -classpath voltkv-client.jar:$CLIENTCLASSPATH voltkv.JDBCBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=$SERVERS \
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
    echo "Usage: ./run.sh {clean|cleanall|jars|server|init|client|async-benchmark|aysnc-benchmark-help|...}"
    echo "       {...|sync-benchmark|sync-benchmark-help|jdbc-benchmark|jdbc-benchmark-help}"
}

# Run the targets pass on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then server; exit; fi
for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done

