#!/usr/bin/env bash

APPNAME="liverejoinconsistency"

# find voltdb binaries (customized from examples to be one level deeper)
if [ -e ../../../bin/voltdb ]; then
    # assume this is the examples folder for a kit
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

LOG4J="$VOLTDB_VOLTDB/log4j.xml"

# remove build artifacts
function clean() {
    rm -rf $APPNAME-client.jar $APPNAME-procs.jar voltdbroot log \
        src/$APPNAME/*.class src/$APPNAME/procedures/*.class
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -classpath $APPCLASSPATH \
        src/$APPNAME/*java src/$APPNAME/procedures/*.java
    # build procedure and client jars
    jar cf $APPNAME-client.jar -C src $APPNAME
    jar cf $APPNAME-procs.jar -C src $APPNAME/procedures
    # remove compiled .class files
    rm -rf src/$APPNAME/*.class src/$APPNAME/procedures/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e $APPNAME-client.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    # note: "init --force" will delete any existing data
    voltdb init --force
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
    java -classpath $APPNAME-client.jar:$APPCLASSPAT sequence.AsyncBenchmark --help
}

function async-benchmark() {
    jars-ifneeded
    java -classpath $APPNAME-client.jar:$APPCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        $APPNAME.AsyncBenchmark \
        --displayinterval=5 \
        --duration=${DURATION:-300} \
        --servers=$SERVERS \
        --ratelimit=${RATE:-100000} \
        --testcase=ADHOCSINGLEPARTPTN
        #--testcase=UPDATEAPPLICATIONCATALOG
        #--testcase=LOADSINGLEPARTITIONTABLEPTN   # this case fails
        #--testcase=ALL
        #--testcase=LOADMULTIPARTITIONTABLEREP
        #--testcase=WRMULTIPARTSTOREDPROCREP
        #--testcase=WRMULTIPARTSTOREDPROCPTN
        #--testcase=WRSINGLEPARTSTOREDPROCPTN
        #--testcase=ADHOCMULTIPARTREP
        #--testcase=ADHOCSINGLEPARTREP
        #--testcase=ADHOCMULTIPARTPTN
}

function verify() {
    jars-ifneeded
    java -classpath $APPNAME-client.jar:$APPCLASSPAT -Dlog4j.configuration=file://$LOG4J \
        $APPNAME.CheckReplicaConsistency \
        --servers=${SERVERS}
}

function simple-benchmark() {
    jars-ifneeded
    java -classpath $APPNAME-client.jar:$APPCLASSPAT -Dlog4j.configuration=file://$LOG4J \
        sequence.SimpleBenchmark localhost
}

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
function sync-benchmark-help() {
    jars-ifneeded
    java -classpath $APPNAME-client.jar:$APPCLASSPAT sequence.SyncBenchmark --help
}

function sync-benchmark() {
    jars-ifneeded
    java -classpath $APPNAME-client.jar:$APPCLASSPAT -Dlog4j.configuration=file://$LOG4J \
        sequence.SyncBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=120 \
        --servers=localhost:21212 \
        --contestants=6 \
        --maxvotes=2 \
        --threads=40
}

# JDBC benchmark sample
# Use this target for argument help
function jdbc-benchmark-help() {
    jars-ifneeded
    java -classpath $APPNAME-client.jar:$APPCLASSPAT sequence.JDBCBenchmark --help
}

function jdbc-benchmark() {
    jars-ifneeded
    java -classpath $APPNAME-client.jar:$APPCLASSPAT -Dlog4j.configuration=file://$LOG4J \
        sequence.JDBCBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --maxvotes=2 \
        --servers=localhost:21212 \
        --contestants=6 \
        --threads=40
}

function help() {
    echo "Usage: ./run.sh {clean|jars|server|init|async-benchmark|aysnc-benchmark-help|...}"
    echo "       {...|sync-benchmark|sync-benchmark-help|jdbc-benchmark|jdbc-benchmark-help}"
}

# Run the targets pass on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then server; exit; fi
for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
