#!/usr/bin/env bash

# This is a semi-clone of examples/voter, with only
# the TracingBenchmark client code. For other client
# targets, use tests/test_apps/voter.
#
# TracingBenchmark doesn't seem to have a whole lot
# to do with the actual voter logic.

echo '-=-=-=-=- test/test_apps/voter-tracing-benchmark -=-=-=-=-'

# find voltdb binaries
if [ -e ../../../bin/voltdb ]; then
    # assume this is the tests/test_apps/voter-tracing-benchmark directory
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

VOTER_BASE=../voter

# remove binaries, logs, runtime artifacts, etc...
function clean() {
    rm -rf voltdbroot log procedures/voter/*.class client/voter/*.class
    rm -rf *.log resTracing*.txt
    rm -rf voter-procs.jar voter-client.jar
}

# compile the source code for procedures and the client into jarfiles
# we merge code from the base test_apps/voter with our specializations
function jars() {
    # compile java source
    javac -classpath $APPCLASSPATH -d procedures $VOTER_BASE/procedures/voter/*.java procedures/voter/*.java
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

# variations on client benchmark code
# 1 - show results on console with detailed latency report
# 2 - show result  on console with main statisical parameters of performance listed in table
# 3 - plot the statistical data in figures
function tracing-benchmark() {
    sqlcmd < ddl.sql > /dev/null # always reset results on server
    java -classpath voter-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
            voter.TracingBenchmark \
            --servers=$SERVERS \
            --outputFormat=${1: -1} \
            --latencyreport=true \
            --ratelimit=20000 \
            --doInsert=true
}

# 1 - show results on console with detailed latency report
function tracing-benchmark-showAll() {
    jars-ifneeded
    tracing-benchmark 1
    voltadmin shutdown > /dev/null
}

function showAll() { # handy abbreviation
    tracing-benchmark-showAll
}

# 3 - plot the statistical data in figures
function tracing-benchmark-figurePlot() {
    jars-ifneeded

    # SET THE NUMBER OF ITERATIONS HERE BY CHANGING THE VALUE OF VARIABLE N
    NUM_ITER=50

    ###################
    # tracing tool off
    ###################

    FILENAME="resTracingOff.txt"
    rm -f $FILENAME

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
    rm -f $FILENAME

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

function figurePlot() { # handy abbreviation
    tracing-benchmark-figurePlot
}

# 2 - show result on console with main statisical parameters of performance listed in table
function tracing-benchmark-showBenchmark() {
    jars-ifneeded

    # SET THE NUMBER OF ITERATIONS HERE BY CHANGING THE VALUE OF VARIABLE N
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

function showBenchmark() { # handy abbreviation
    tracing-benchmark-showBenchmark
}

function help() {
    echo "
  Usage: ./run.sh TARGET

  Targets:
        jars | init | clean
        server (starts a local server)
        tracing-benchmark-showAll -or- showAll
        tracing-benchmark-showBenchmark -or- showBenchmark
        tracing-benchmark-figurePlot -or- figurePlot
        test-volt-trace

"
}

# Not really sure of the point here
function test-volt-trace() {
    jars-ifneeded
    sqlcmd < ddl.sql
    sqlcmd --query="exec @Trace status"
    sqlcmd --query="exec @Trace filter 800"
    sqlcmd --query="exec @Trace enable SPI"
    sqlcmd --query="exec @Trace enable CI"
    sqlcmd --query="exec @Trace status"
    tracing-benchmark 1 # showAll
    sqlcmd --query="exec @Trace dump"
    sqlcmd --query="exec @Trace filter 0"
    sqlcmd --query="exec @Trace status"
    sqlcmd --query="exec @Trace disable ALL"
    #sqlcmd --query="exec @Statistics procedureprofile 0"
    voltadmin shutdown
}

# Run the targets pass on the command line

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
