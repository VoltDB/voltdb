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

VOLTDB_BASE=$VOLTDB_HOME
VOLTDB_LIB=$VOLTDB_HOME/lib
VOLTDB_BIN=$VOLTDB_HOME/bin
VOLTDB_VOLTDB=$VOLTDB_HOME/voltdb

PATH=$VOLTDB_BIN:$PATH

CLIENTLOG4J="log4j.properties"
APPCLASSPATH=$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
CLIENTCLASSPATH=txnid-client.jar:txnid-procs.jar:$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/commons-cli-1.2.jar; \
} 2> /dev/null | paste -sd ':' - )
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove binaries, logs, runtime artifacts, etc... but keep the jars
function clean() {
    rm -rf voltdbroot log procedures/txnIdSelfCheck/*.class client/txnIdSelfCheck/*.class *.jar
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    clean
    rm -rf txnid-procs.jar txnid-client.jar
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -target 1.7 -source 1.7 -classpath $APPCLASSPATH procedures/txnIdSelfCheck/*.java
    jar cf txnid-procs.jar -C procedures txnIdSelfCheck
    javac -target 1.7 -source 1.7 -classpath txnid-procs.jar:$CLIENTCLASSPATH client/txnIdSelfCheck/*.java
    jar cf txnid-client.jar -C client txnIdSelfCheck
    # remove compiled .class files
    rm -rf procedures/txnIdSelfCheck/*.class client/txnIdSelfCheck/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e txnid-procs.jar ] || [ ! -e txnid-client.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command line: "
    echo
    echo "voltdb create -d deployment.xml -l $LICENSE -H $HOST"
    echo
    voltdb create -d deployment.xml -l $LICENSE -H $HOST
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
    # run the server in the background
    voltdb create -B -d deployment.xml -l $LICENSE -H $HOST > nohup.log 2>&1 &
    wait_for_startup
    init
}

# run the client that drives the example
function client() {
    benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function benchmark-help() {
    jars-ifneeded
    java -classpath $CLIENTCLASSPATH txnIdSelfCheck.Benchmark --help
}

function benchmark() {
    jars-ifneeded
    java -ea -classpath $CLIENTCLASSPATH -Dlog4j.configuration=$CLIENTLOG4J -Dlog4j.debug \
        txnIdSelfCheck.Benchmark \
        --displayinterval=1 \
        --duration=120 \
        --servers=localhost \
        --threads=20 \
        --threadoffset=0 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --entropy=127 \
        --fillerrowsize=10240 \
        --replfillerrowmb=32 \
        --partfillerrowmb=128 \
        --progresstimeout=120 \
        --usecompression=false \
        --allowinprocadhoc=true
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
    echo "Usage: ./run.sh {clean|server|init|demo|client|benchmark|benchmark-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
