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
    rm -rf voltdbroot log procedures/geospatial/*.class client/geospatial/*.class
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    clean
    rm -rf geospatial-procs.jar geospatial-client.jar
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -classpath $APPCLASSPATH procedures/geospatial/*.java
    javac -classpath $CLIENTCLASSPATH client/geospatial/*.java
    # build procedure and client jars
    jar cf geospatial-procs.jar -C procedures geospatial
    jar cf geospatial-client.jar -C client geospatial
    # remove compiled .class files
    rm -rf procedures/geospatial/*.class client/geospatial/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e geospatial-procs.jar ] || [ ! -e geospatial-client.jar ]; then
        jars;
    fi
}

# load schema, procedures, and static data
function init() {
    jars-ifneeded
    sqlcmd < ddl.sql
    csvloader -f advertisers.csv advertisers
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

# run this target to see what command line options the client offers
function client-help() {
    jars-ifneeded
    java -classpath geospatial-client.jar:$CLIENTCLASSPATH \
        geospatial.AdBrokerBenchmark --help
}

# run the client that drives the example
function client() {
    jars-ifneeded
    java -classpath geospatial-client.jar:$CLIENTCLASSPATH geospatial.AdBrokerBenchmark
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
