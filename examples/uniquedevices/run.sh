#!/usr/bin/env bash

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
    rm -rf voltdbroot log \
        hyperloglogsrc/uniquedevices/org/voltdb/hll/*.class \
        procedures/uniquedevices/*.class \
        client/uniquedevices/*.class
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    clean
    rm -rf uniquedevices-procs.jar uniquedevices-client.jar
}

function webserver() {
    cd web; python -m SimpleHTTPServer $WEB_PORT
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac \
        hyperloglogsrc/org/voltdb_hll/HyperLogLog.java \
        hyperloglogsrc/org/voltdb_hll/MurmurHash.java \
        hyperloglogsrc/org/voltdb_hll/RegisterSet.java
    javac -classpath hyperloglogsrc:$APPCLASSPATH \
        procedures/uniquedevices/*.java
    javac -classpath hyperloglogsrc:$CLIENTCLASSPATH \
        client/uniquedevices/*.java
    # build procedure and client jars
    jar cf uniquedevices-procs.jar -C procedures uniquedevices
    jar uf uniquedevices-procs.jar -C hyperloglogsrc org
    jar cf uniquedevices-client.jar -C client uniquedevices
    jar uf uniquedevices-client.jar -C hyperloglogsrc org
    # remove compiled .class files
    rm -rf \
        procedures/uniquedevices/*.class \
        hyperloglogsrc/org/org/voltdb_hll/*.class \
        client/uniquedevices/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e uniquedevices-procs.jar ] || [ ! -e uniquedevices-client.jar ]; then
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

# run the client that drives the example
function client() {
    jars-ifneeded
    java -classpath uniquedevices-client.jar:$CLIENTCLASSPATH uniquedevices.UniqueDevicesClient \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost:21212 \
        --appcount=100
}

# Asynchronous benchmark sample
# Use this target for argument help
function client-help() {
    jars-ifneeded
    java -classpath uniquedevices-client.jar:$CLIENTCLASSPATH uniquedevices.UniqueDevicesClient --help
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
