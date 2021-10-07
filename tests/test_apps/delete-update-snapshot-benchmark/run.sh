#!/usr/bin/env bash

APPNAME="delete-update-snapshot"
if [ -z $CONFIGFILE]; then
    # default value
    CONFIGFILE="deployment.xml"
fi

# leader host for startup purposes only
# (once running, all nodes are the same -- no leaders)
STARTUPLEADERHOST="localhost"
# list of cluster nodes separated by commas in host:[port] format
SERVERS="localhost"

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

# remove binaries, logs, runtime artifacts, etc... but keep the jars
function clean() {
    rm -rf voltdbroot log src/procedures/*.class src/client/benchmark/*.class
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    clean
    rm -rf dusbench.jar
}


# compile the source code for procedures and the client into a jar file
function jars() {
    # compile java source
    javac -classpath $CLIENTCLASSPATH src/client/benchmark/*.java
    javac -classpath src:$APPCLASSPATH src/procedures/*.java
    # build procedure and client jars
    jar cf dusbench.jar -C src procedures/  -C src client/benchmark/
    # remove compiled .class files
    rm -rf src/procedures/*.class src/client/benchmark/*.class
}

# compile the jar file if it doesn't exist
function jars-if-needed() {
    if [ ! -e dusbench.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    # note: "init --force" will delete any existing data
    # omit the "init" command to recover existing data
    voltdb init --force -C $CONFIGFILE $ARGS
    voltdb start -H $STARTUPLEADERHOST
}

# run the voltdb server locally, with auto-snapshots
function server-auto-snapshot() {
    CONFIGFILE="deployment-auto-snapshot.xml"
    server
}

# load schema and procedures
function init() {
    jars-if-needed
    sqlcmd < ddl.sql
}

# run this target to see what command line options the client offers
function client-help() {
    jars-if-needed
    java -classpath dusbench.jar:$CLIENTCLASSPATH client.benchmark.DUSBenchmark --help
}

# run the client that drives the example with some editable options
# Here we run the tests with both inline and outofline data.
function client() {
    local DONE
    local ARG
    jars-if-needed
    java -classpath dusbench.jar:$CLIENTCLASSPATH client.benchmark.DUSBenchmark $ARGS
}

function help() {
    echo "Usage: ./run.sh {clean|cleanall|jars|jars-if-needed|server|server-auto-snapshot|init|client|client-help|help}"
}

# If no first arg, run help
if [ $# -eq 0 ]; then help; exit; fi

# Run the targets passed on the command line
while [ -n "$1" ] ; do
    CMD="$1"
    ARGS=
    if [[ "$1" == "client" || "$1" == server* ]]; then
        while [[ "$2" == "--"* ]]; do
            ARGS="$ARGS $2 $3"
            shift
            shift
        done
    fi
    if [[ "$1" != "help" ]]; then
        echo "$0 Performing: $CMD$ARGS"
    fi
    $CMD
    shift
done
