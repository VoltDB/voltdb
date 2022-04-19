#!/usr/bin/env bash

# This is a clone of examples/voter, intended for use
# in VoltDB testing, rather than as a VoltDB tutorial.

echo '-=-=-=-=- test/test_apps/voter -=-=-=-=-'

# find voltdb binaries
if [ -e ../../../bin/voltdb ]; then
    # assume this is the tests/test_apps/voter directory
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

# remove binaries, logs, runtime artifacts, etc... but keep the jars
function clean() {
    rm -rf voltdbroot log procedures/voter/*.class client/voter/*.class *.log
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    clean
    rm -rf voter-procs.jar voter-client.jar
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -classpath $APPCLASSPATH procedures/voter/*.java
    javac -classpath $CLIENTCLASSPATH client/voter/*.java
    # build procedure and client jars
    jar cf voter-procs.jar -C procedures voter
    jar cf voter-client.jar -C client voter
    # remove compiled .class files
    rm -rf procedures/voter/*.class client/voter/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e voter-procs.jar ] || [ ! -e voter-client.jar ]; then
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

version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
add_open=
if [[ $version == 11.0* ]] || [[ $version == 17.0* ]] ; then
        add_open="--add-opens java.base/sun.nio.ch=ALL-UNNAMED"
fi

# run the client that drives the example
function client() {
    async-benchmark
}

# run the non-blocking version
function nbclient() {
    nonblocking-benchmark
}

# run the 'Client2' version
function client2() {
    client2-async-benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    jars-ifneeded
    java -classpath voter-client.jar:$CLIENTCLASSPATH voter.AsyncBenchmark --help
}

# latencyreport: default is OFF
# ratelimit: must be a reasonable value if lantencyreport is ON
function async-benchmark() {
    jars-ifneeded
    java $add_open \
	-classpath voter-client.jar:$CLIENTCLASSPATH voter.AsyncBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=120 \
        --servers=$SERVERS \
        --contestants=6 \
        --maxvotes=2 \
        --affinityreport=false
}

# latencyreport: default is OFF
# ratelimit: must be a reasonable value if latencyreport is ON
function nonblocking-benchmark() {
    jars-ifneeded
    java $add_open \
	-classpath voter-client.jar:$CLIENTCLASSPATH voter.NonblockingAsyncBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=120 \
        --servers=$SERVERS \
        --contestants=6 \
        --maxvotes=2
}

# Client2 variants

function client2-async-benchmark() {
    jars-ifneeded
    java $add_open \
	-classpath voter-client.jar:$CLIENTCLASSPATH voter.Client2AsyncBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=120 \
        --servers=$SERVERS \
        --contestants=6 \
        --maxvotes=2 \
        --affinityreport=false
}

function client2-sync-benchmark() {
    jars-ifneeded
    java $add_open \
	-classpath voter-client.jar:$CLIENTCLASSPATH voter.Client2SyncBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=120 \
        --servers=$SERVERS \
        --contestants=6 \
        --maxvotes=2 \
        --threads=40
}

# trivial client code for illustration purposes
function simple-benchmark() {
    jars-ifneeded
    java -classpath voter-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        voter.SimpleBenchmark $SERVERS
}

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
function sync-benchmark-help() {
    jars-ifneeded
    java -classpath voter-client.jar:$CLIENTCLASSPATH voter.SyncBenchmark --help
}

function sync-benchmark() {
    jars-ifneeded
    java $add_open \
	-classpath voter-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        voter.SyncBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=120 \
        --servers=$SERVERS \
        --contestants=6 \
        --maxvotes=2 \
        --threads=40
}

# JDBC benchmark sample
# Use this target for argument help
function jdbc-benchmark-help() {
    jars-ifneeded
    java -classpath voter-client.jar:$CLIENTCLASSPATH voter.JDBCBenchmark --help
}

function jdbc-benchmark() {
    jars-ifneeded
    java $add_open \
	-classpath voter-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        voter.JDBCBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=$SERVERS \
        --maxvotes=2 \
        --contestants=6 \
        --threads=40
}

# AdHoc benchmark, previously in test_apps/voter-adhoc.
function adhoc-benchmark() {
    java $add_open \
	-classpath voter-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        voter.AdHocBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=12 \
        --servers=localhost:21212 \
        --contestants=6 \
        --maxvotes=2
}

function help() {
    echo "
Usage: ./run.sh target...

General targets:
        help | clean | cleanall | jars | jars-ifneeded |
        init | voltinit-ifneeded | server

Benchmark targets:
        async-benchmark | nonblocking-benchmark | sync-benchmark | jdbc-benchmark |
        client2-async-benchmark | client2-sync-benchmark | adhoc-benchmark | simple-benchmark

Selected help:
         async-benchmark-help | sync-benchmark-help | jdbc-benchmark-help

Abbreviations:
        client   : async-benchmark
        nbclient : nonblocking-benchmark
        client2  : client2-async-benchmark
"
}

# Run the targets passed on the command line

if [ $# -eq 0 ];
then
    help
    exit 0
fi

for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
