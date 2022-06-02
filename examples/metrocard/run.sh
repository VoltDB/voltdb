#!/usr/bin/env bash

# leader host for startup purposes only
# (once running, all nodes are the same -- no leaders)
STARTUPLEADERHOST="localhost"
# list of cluster nodes separated by commas in host:[port] format
SERVERS="localhost"

# WEB SERVER variables
WEB_PORT=8081

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
    rm -rf voltdbroot log procedures/metrocard/*.class client/metrocard/*.class
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    clean
    rm -rf metrocard-procs.jar metrocard-client.jar
}

function webserver() {
    cd web; python -m SimpleHTTPServer $WEB_PORT
}

function start_export_web() {
    cd exportWebServer; python exportServer.py
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -classpath $APPCLASSPATH procedures/metrocard/*.java
    javac -classpath $CLIENTCLASSPATH client/metrocard/*.java
    # build procedure and client jars
    jar cf metrocard-procs.jar -C procedures metrocard
    jar cf metrocard-client.jar -C client metrocard
    # remove compiled .class files
    rm -rf procedures/metrocard/*.class client/metrocard/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e metrocard-procs.jar ] || [ ! -e metrocard-client.jar ]; then
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
    echo "----Loading Stations----"
    csvloader --servers $SERVERS --file data/stations.csv --reportdir log stations
}

# run this target to see what command line options the client offers
function client-help() {
    jars-ifneeded
    java -classpath metrocard-client.jar:$CLIENTCLASSPATH metrocard.metrocardApp --help
}

# run the client that drives the example with some editable options
function client() {
    jars-ifneeded
    java -classpath metrocard-client.jar:$CLIENTCLASSPATH metrocard.MetroBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=120 \
        --servers=$SERVERS \
        --ratelimit=250000 \
        --cardcount=50000
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
