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
    rm -rf voltdbroot log procedures/windowing/*.class client/windowing/*.class
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    clean
    rm -rf windowing-procs.jar windowing-client.jar
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -classpath $APPCLASSPATH procedures/windowing/*.java
    javac -classpath $CLIENTCLASSPATH client/windowing/*.java
    # build procedure and client jars
    jar cf windowing-procs.jar -C procedures windowing
    jar cf windowing-client.jar -C client windowing
    # remove compiled .class files
    rm -rf procedures/windowing/*.class client/windowing/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e windowing-procs.jar ] || [ ! -e windowing-client.jar ]; then
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

# run this target to see what command line options the client offers
function client-help() {
    jars-ifneeded
    java -classpath windowing-client.jar:$CLIENTCLASSPATH windowing.WindowingApp --help
}

# run the client that drives the example with some editable options
function client() {
    jars-ifneeded
    # Note that in the command below, maxrows and historyseconds can't both be non-zero.
    java -classpath windowing-client.jar:$CLIENTCLASSPATH windowing.WindowingApp \
        --displayinterval=5 \
        --duration=120 \
        --servers=$SERVERS \
        --maxrows=0 \
        --historyseconds=30 \
        --inline=false \
        --deletechunksize=100 \
        --deleteyieldtime=100 \
        --ratelimit=15000
}

function help() {
    echo "Usage: ./run.sh {clean|cleanall|jars|server|init|client|client-help}"
}

# Run the targets pass on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then server; exit; fi
for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
