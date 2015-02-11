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
CLIENTCLASSPATH=uniquedevices-client.jar:$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdbclient-*.jar; \
    \ls -1 "$VOLTDB_LIB"/commons-cli-1.2.jar; \
} 2> /dev/null | paste -sd ':' - )
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

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

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -target 1.7 -source 1.7 \
        hyperloglogsrc/org/voltdb/hll/HyperLogLog.java \
        hyperloglogsrc/org/voltdb/hll/MurmurHash.java \
        hyperloglogsrc/org/voltdb/hll/RegisterSet.java
    javac -target 1.7 -source 1.7 -classpath hyperloglogsrc:$APPCLASSPATH \
        procedures/uniquedevices/*.java
    javac -target 1.7 -source 1.7 -classpath hyperloglogsrc:$CLIENTCLASSPATH \
        client/uniquedevices/*.java
    # build procedure and client jars
    jar cf uniquedevices-procs.jar -C procedures uniquedevices
    jar uf uniquedevices-procs.jar -C hyperloglogsrc org
    jar cf uniquedevices-client.jar -C client uniquedevices
    jar uf uniquedevices-client.jar -C hyperloglogsrc org
    # remove compiled .class files
    rm -rf \
        procedures/uniquedevices/*.class \
        hyperloglogsrc/org/org/voltdb/hll/*.class \
        client/uniquedevices/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e uniquedevices-procs.jar ] || [ ! -e uniquedevices-client.jar ]; then
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
    jars-ifneeded
    java -classpath $CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        uniquedevices.UniqueDevicesClient \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost:21212 \
        --appcount=100
}

# Asynchronous benchmark sample
# Use this target for argument help
function client-help() {
    jars-ifneeded
    java -classpath $CLIENTCLASSPATH uniquedevices.UniqueDevicesClient --help
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
    echo "Usage: ./run.sh {clean|server|init|demo|client|async-benchmark|aysnc-benchmark-help|...}"
    echo "       {...|sync-benchmark|sync-benchmark-help|jdbc-benchmark|jdbc-benchmark-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
