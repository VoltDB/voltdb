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
    rm -rf voltdbroot log procedures/debitcredit/*.class client/voter/*.class *.log
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    clean
    rm -rf debitcredit-procs.jar voter-client.jar
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -classpath $APPCLASSPATH procedures/debitcredit/*.java
    javac -classpath $CLIENTCLASSPATH client/debitcredit/*.java
    # build procedure and client jars
    jar cf debitcredit-procs.jar -C procedures debitcredit
    jar cf debitcredit-client.jar -C client debitcredit
    # remove compiled .class files
    rm -rf procedures/debitcredit/*.class client/debitcredit/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e debitcredit-procs.jar ] || [ ! -e debitcredit-client.jar ]; then
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
    init
    benchmark
}

function benchmark() {
    java -classpath debitcredit-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        debitcredit.DebitCreditBenchmark \
        --type='MP' \
        --transferpct=5 \
        --ratelimit=20000 
#    java -classpath debitcredit-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
#        debitcredit.DebitCreditBenchmark \ 
#        --displayinterval=5 \
#        --warmup=1 \
#        --duration=20 \
#        --servers=$SERVERS \
#        --ratelimit=20000 \
#        --latencytarget=6
}

function help() {
    echo "Usage: ./run.sh {clean|cleanall|jars|server|init|client}"
}

# Run the targets pass on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then server; exit; fi
for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done

