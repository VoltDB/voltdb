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

# remove binaries, logs, runtime artifacts, etc... but keep the jars
function clean() {
    rm -rf voltdbroot log procedures/polygonBenchmark/*.class client/polygonBenchmark/*.class *.log
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    clean
    rm -rf polygonBenchmark-procs.jar polygonBenchmark-client.jar
}

function printClassPath() {
    for c in $(echo $1 | tr ':' ' '); do
    	echo "    $c"
    done
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -classpath $APPCLASSPATH procedures/polygonBenchmark/*.java
    javac -classpath $CLIENTCLASSPATH client/polygonBenchmark/*.java
    # build procedure and client jars
    # jar cf polygonBenchmark-procedures.jar -C procedures polygonBenchmark
    jar cf polygonBenchmark-client.jar -C client polygonBenchmark
    jar cf polygonBenchmark-procedures.jar -C procedures polygonBenchmark
    # remove compiled .class files
    rm -rf procedures/polygonBenchmark/*.class client/polygonBenchmark/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e polygonBenchmark-procedures.jar ] || [ ! -e polygonBenchmark-client.jar ]; then
        jars;
    fi
}

# Init to directory voltdbroot
function voltinit-ifneeded() {
    voltdb init --force
}

# run the voltdb server locally
function server() {
    jars-ifneeded
    voltinit-ifneeded
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
    java -classpath polygonBenchmark-client.jar:$CLIENTCLASSPATH polygonBenchmark.AsyncBenchmark --help
}

function abend() {
	local VBL="$1"
    echo "$0: Envariable $VBL must be defined"
}

# latencyreport: default is OFF
# ratelimit: must be a reasonable value if lantencyreport is ON
# Disable the comments to get latency report
function async-benchmark() {
    jars-ifneeded
    if [ -z "$REPAIR_FRAC" ] ; then
        abend REPAIR_FRAC
        exit 100
    fi
    if [ -z "$INSERT_FUNCTION" ] ; then
        abend INSERT_FUNCTION
        exit 100
    fi
    if [ -z "$NUM_VERTICES" ] ; then
        abend NUM_VERTICES
        exit 100
    fi
    if [ -z "$DURATION" ] ; then
        abend DURATION
        exit 100
    fi
    if [ -z "$LOG_FILE" ] ; then
        abend LOG_FILE
        exit 100
    fi
    echo "Logging to file $LOG_FILE"
    java -classpath polygonBenchmark-client.jar:$CLIENTCLASSPATH polygonBenchmark.AsyncBenchmark \
        --repairFrac=$REPAIR_FRAC \
        --displayinterval=5 \
        --warmup=0 \
        --insertFunction="$INSERT_FUNCTION" \
        --vertices="$NUM_VERTICES" \
        --duration=30 \
        --servers=$SERVERS |& tee -a "$LOG_FILE"
}

function help() {
    echo "Usage: ./run.sh {clean|cleanall|jars|server|init|client|async-benchmark|aysnc-benchmark-help|...}"
    echo "       {...|sync-benchmark|sync-benchmark-help|jdbc-benchmark|jdbc-benchmark-help|simple-benchmark}"
}

# Run the targets pass on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then server; exit; fi
for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
