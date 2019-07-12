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

# run the client that drives the example
function client() {
    async-benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    jars-ifneeded
    java -classpath voter-client.jar:$CLIENTCLASSPATH voter.AsyncBenchmark --help
}

# latencyreport: default is OFF
# ratelimit: must be a reasonable value if lantencyreport is ON
# Disable the comments to get latency report
function async-benchmark() {
    jars-ifneeded
    java -classpath voter-client.jar:$CLIENTCLASSPATH voter.AsyncBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=120 \
        --servers=$SERVERS \
        --contestants=6 \
        --maxvotes=2 \
        --latencyreport=true \
        --ratelimit=10000
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
    java -classpath voter-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
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
    java -classpath voter-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        voter.JDBCBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=$SERVERS \
        --maxvotes=2 \
        --contestants=6 \
        --threads=40
}

function start-voltdb() {
    voltdb init --force --dir=.
    voltdb start --dir=.
}

function tracing-benchmark() {
    sqlcmd < ddl.sql > /dev/null
    java -classpath voter-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
            voter.TracingBenchmark \
            --servers=$SERVERS \
            --outputFormat=${1: -1} \
            --latencyreport=true \
            --ratelimit=20000 \
            --doInsert=true
}

function tracing-benchmark-showAll() {
    tracing-benchmark 1
    #voltadmin shutdown
}

function tracing-benchmark-figurePlot() {
    jars-ifneeded
    # SET THE NUMBER OF ITERATIONS HERE BY CHANGE THE VALUE OF VARIABLE N
    NUM_ITER=50

    ###################
    # tracing tool off
    ###################
    FILENAME="resTracingOff.txt"
    if [ -f ./$FILENAME ]; then
        echo "file already exists; remove it"
        rm $FILENAME
    fi

    for i in $(eval echo "{1..$NUM_ITER}")
    do
        tracing-benchmark 3 >> $FILENAME
        #sqlcmd --query="exec @Statistics procedureprofile 1;"
        sqlcmd --query="delete from votes" > /dev/null
    done

    ###################
    # tracing tool on
    ###################
    sqlcmd --query="exec @Trace status" > /dev/null
    sqlcmd --query="exec @Trace enable CI" > /dev/null
    sqlcmd --query="exec @Trace enable SPI" > /dev/null
    #sqlcmd --query="exec @Trace enable EE" > /dev/null
    sqlcmd --query="exec @Trace status" > /dev/null

    FILENAME="resTracingOn.txt"
    if [ -f ./$FILENAME ]; then
        echo "file already exists; remove it"
        rm $FILENAME
    fi

    for i in $(eval echo "{1..$NUM_ITER}")
    do
        tracing-benchmark 3 >> $FILENAME
        #sqlcmd --query="exec @Statistics procedureprofile 1;"
        sqlcmd --query="delete from votes" > /dev/null
    done

    sqlcmd --query="exec @Trace disable CI" > /dev/null
    sqlcmd --query="exec @Trace disable SPI" > /dev/null
    #sqlcmd --query="exec @Trace disable EE" > /dev/null
    sqlcmd --query="exec @Trace status" > /dev/null
    # shutdown database
    voltadmin shutdown > /dev/null

    # call python to plot figure
    python3 tracingBenchmarkPlot.py
    # remove file
    rm resTracing*.txt
}

function tracing-benchmark-showBenchmark() {
    jars-ifneeded
    # SET THE NUMBER OF ITERATIONS HERE BY CHANGE THE VALUE OF VARIABLE N
    NUM_ITER=10

    echo "-----------------------------------------"
    echo "Benchmarking (tracing tool turned off):"
    echo "-----------------------------------------"

    echo "Throughput (txns/sec)      Avg Latency (ms)        99.9% Latency (ms)      Max Latency (ms)"
    echo "---------------------      ------------------      ------------------      -----------------"
    for i in $(eval echo "{1..$NUM_ITER}")
    do
        tracing-benchmark 2
        #sqlcmd --query="exec @Statistics procedureprofile 1;"
        sqlcmd --query="delete from votes" > /dev/null
    done

    echo "----------------------------------------"
    echo "Benchmarking (tracing tool turned on):"
    echo "----------------------------------------"

    echo "Throughput (txns/sec)      Avg Latency (ms)        99.9% Latency (ms)      Max Latency (ms)"
    echo "---------------------      ------------------      ------------------      -----------------"
    sqlcmd --query="exec @Trace status" > /dev/null
    sqlcmd --query="exec @Trace enable CI" > /dev/null
    sqlcmd --query="exec @Trace enable SPI" > /dev/null
    #sqlcmd --query="exec @Trace enable EE" > /dev/null
    sqlcmd --query="exec @Trace status" > /dev/null

    for i in $(eval echo "{1..$NUM_ITER}")
    do
        tracing-benchmark 2
        #sqlcmd --query="exec @Statistics procedureprofile 1;"
        sqlcmd --query="delete from votes" > /dev/null
    done

    sqlcmd --query="exec @Trace disable CI" > /dev/null
    sqlcmd --query="exec @Trace disable SPI" > /dev/null
    #sqlcmd --query="exec @Trace disable EE" > /dev/null
    sqlcmd --query="exec @Trace status" > /dev/null

    voltadmin shutdown > /dev/null
}

function help() {
    echo "Usage: ./run.sh {clean|cleanall|jars|server|init|client|async-benchmark|aysnc-benchmark-help|...}"
    echo "       {...|sync-benchmark|sync-benchmark-help|jdbc-benchmark|jdbc-benchmark-help|simple-benchmark}"
}

function test-volt-trace() {
    sqlcmd < ddl.sql
    sqlcmd --query="exec @Trace status"
    #sqlcmd --query="exec @Trace filter 0"
    sqlcmd --query="exec @Trace enable SPI"
    #sqlcmd --query="exec @Trace enable CI"
    sqlcmd --query="exec @Trace status"
    tracing-benchmark-showAll
    #sqlcmd --query="exec @Trace filter 0"
    sqlcmd --query="exec @Trace status"
    sqlcmd --query="exec @Trace dump"
    sqlcmd --query="exec @Trace disable ALL"
    #sqlcmd --query="exec @Statistics procedureprofile 0"
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
