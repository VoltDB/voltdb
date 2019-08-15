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

# compile the source code for procedures and the client into jarfiles
function jars-client() {
    javac -classpath $CLIENTCLASSPATH client/udaf/*.java
    jar cf udaf-client.jar -C client udaf
    rm -rf client/udaf/*.class
}

function jars-procedures() {
    javac -classpath $APPCLASSPATH procedures/udaf/*.java
    jar cf udaf-procs.jar -C procedures udaf
    rm -rf procedures/udaf/*.class
}

function jars-udafs() {
    javac -classpath "$HOME/Documents/Working/voltdb/voltdb/*" udafs/*.java
    jar cvf myudafs.jar -C udafs .
    rm -rf udafs/*.class
}

function jars() {
    jars-client
    jars-procedures
    jars-udafs
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e udaf-procs.jar ] || [ ! -e udaf-client.jar ] || [ ! -e myudafs.jar ]; then
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
    sqlcmd < ddl.sql > /dev/null
}

function start-voltdb() {
    voltdb init --force --dir=.
    voltdb start --dir=.
}

function load-schema() {
    sqlcmd < ddl.sql > /dev/null
}

function udaf-benchmark() {
    java -classpath udaf-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        udaf.UDAFBenchmark \
        --servers=$SERVERS \
        --latencyreport=true \
        --ratelimit=20000 \
        --doCount=true \
        --outputFormat=${1: -1} \
        --tablesize=${2}
}

function udaf-benchmark-latencyReport() {
    init
    udaf-benchmark 1 200
    voltadmin shutdown
}

function udaf-benchmark-showStatsResults() {
    init
    echo "Table Size           Throughput (txns/sec)         Avg Latency (ms)     "
    echo "---------------      ------------------------      -------------------- "
    for tbsize in 100 500 1000 5000 10000 50000 100000
    do
        udaf-benchmark 2 $tbsize
    done
    voltadmin shutdown
}

function run-benchmark() {
    init
    udaf-benchmark
    voltadmin shutdown
}

# Run the targets pass on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then server; exit; fi
for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done