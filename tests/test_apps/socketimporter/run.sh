#!/usr/bin/env bash

#set -o nounset #exit if an unset variable is used
set -o errexit #exit on any single command fail

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(dirname $(dirname $(pwd)))/bin"
    echo "The VoltDB scripts are not in your PATH."
    echo "For ease of use, add the VoltDB bin directory: "
    echo
    echo $VOLTDB_BIN
    echo
    echo "to your PATH."
    echo
fi
# move voltdb commands into path for this script
PATH=$VOLTDB_BIN:$PATH

# installation layout has all libraries in $VOLTDB_ROOT/lib/voltdb
if [ -d "$VOLTDB_BIN/../lib/voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib/voltdb"
    VOLTDB_VOLTDB="$VOLTDB_LIB"
# distribution layout has libraries in separate lib and voltdb directories
else
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib"
    VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"
fi

APPCLASSPATH=$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
CLIENTCLASSPATH=socketstream-client.jar:$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdbclient-*.jar; \
    \ls -1 "$VOLTDB_LIB"/commons-lang3-3.8.1.jar; \
} 2> /dev/null | paste -sd ':' - )
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove binaries, logs, runtime artifacts, etc... but keep the jars
function clean() {
    rm -rf debugoutput voltdbroot log catalog-report.html \
         statement-plans build/*.class clientbuild/*.class
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    ant clean
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    ant client
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    rm -rf felix-cache
    if [ ! -e socketstream-client.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    jars-ifneeded
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command lines: "
    echo
    echo "voltdb init -C deployment.xml -l $LICENSE"
    echo "voltdb start -H $HOST"
    echo
    voltdb init -C deployment.xml -l $LICENSE
    voltdb start -H $HOST
}

#kafka importer
function kafka() {
    jars-ifneeded
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command lines: "
    echo
    echo "voltdb init -C deployment-kafka.xml -l $LICENSE"
    echo "voltdb start -H $HOST"
    echo
    voltdb init -C deployment-kafka.xml -l $LICENSE
    voltdb start -H $HOST
}

#log4j importer
function log4j() {
    jars-ifneeded
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command lines: "
    echo
    echo "voltdb init -C deployment-log4j.xml -l $LICENSE"
    echo "voltdb start -H $HOST"
    echo
    voltdb init -C deployment-log4j.xml -l $LICENSE
    voltdb start -H $HOST
}

#all importer
function all() {
    jars-ifneeded
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command lines: "
    echo
    echo "voltdb init -C deployment-all.xml -l $LICENSE"
    echo "voltdb start -H $HOST"
    echo
    voltdb init -C deployment-all.xml -l $LICENSE
    voltdb start -H $HOST
}

# load schema and procedures
function init() {
    jars-ifneeded
    sqlcmd < ddl.sql
}

# wait for backgrounded server to start up
function wait_for_startup() {
    until sqlcmd  --query=' exec @SystemInformation, OVERVIEW;' > /dev/null 2>&1
    do
        sleep 2
        echo " ... Waiting for VoltDB to start"
        if [[ $SECONDS -gt 60 ]]
        then
            echo "Exiting.  VoltDB did not startup within 60 seconds" 1>&2; exit 1;
        fi
    done
}

# startup server in background and load schema
function background_server_andload() {
    jars-ifneeded
    # run the server in the background
    voltdb init -C deployment.xml -l $LICENSE > nohup.log 2>&1
    voltdb start -B -H $HOST >> nohup.log 2>&1
    wait_for_startup
    init
}

# run the client that drives the example
function client() {
    async-benchmark
}

# run the client that drives the example
function sclient() {
    simple-benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    jars-ifneeded
    java -classpath $CLIENTCLASSPATH socketstream.AsyncBenchmark --help
}

# latencyreport: default is OFF
# ratelimit: must be a reasonable value if lantencyreport is ON
# Disable the comments to get latency report
function async-benchmark() {
    jars-ifneeded
    java -classpath $CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        socketimporter.client.socketimporter.AsyncBenchmark \
        --displayinterval=5 \
        --warmup=2 \
        --duration=30 \
        --perftest=true \
        --partitioned=true \
        --servers=localhost \
        --sockservers=localhost:7001
}

function simple-benchmark() {
    jars-ifneeded
    java -classpath $CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        socketimporter.client.socketimporter.SimpleAsyncBenchmark \
        --displayinterval=5 \
        --warmup=2 \
        --duration=90 \
        --servers=localhost \
        --sockservers=localhost:7001 \
        --statsfile=abc.csv
}


# The following two demo functions are used by the Docker package. Don't remove.
# compile the jars for procs and client code
function demo-compile() {
    jars
}

function demo() {
    echo "starting server in background..."
    background_server_andload
    echo "starting client..."
    client

    echo
    echo When you are done with the demo database, \
        remember to use \"voltadmin shutdown\" to stop \
        the server process.
}

function help() {
    echo "Usage: ./run.sh {clean|server|init|demo|client|async-benchmark|aysnc-benchmark-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
