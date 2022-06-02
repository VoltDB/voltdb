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
    sqlcmd --servers=$SERVERS < ddl.sql
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

# trivial client code for illustration purposes
function simple-benchmark() {
    jars-ifneeded
    java -classpath voter-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        voter.SimpleBenchmark $SERVERS
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
        --maxvotes=2
}

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
function sync-benchmark-help() {
    jars-ifneeded
    java -classpath voter-client.jar:$CLIENTCLASSPATH voter.SyncBenchmark --help
}

function sync-benchmark() {
    jars-ifneeded
    java $add_opens \
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

function help() {
    echo "
  Usage: ./run.sh [TARGET...]

  Targets:
     help | clean | cleanall | jars | init |
     server | client |
     async-benchmark | async-benchmark-help |
     sync-benchmark | sync-benchmark-help |
     jdbc-benchmark | jdbc-benchmark-help |
     simple-benchmark

  The default target is 'server'.
  The 'client' target is the same as 'async-benchmark'.

"
}

# Run the targets pass on the command line
# If no first arg, run server
if [ $# -eq 0 ];
then
    server;
    exit;
fi

for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
