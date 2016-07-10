#!/usr/bin/env bash

APPNAME="ycsb"
CLIENTNAME="$APPNAME"_client

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(pwd)/../../../bin"
fi
# installation layout has all libraries in $VOLTDB_ROOT/lib/voltdb
if [ -d "$VOLTDB_BIN/../lib/voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib/voltdb"
    VOLTDB_VOLTDB="$VOLTDB_LIB"
# distribution layout has libraries in separate lib and voltdb directories
elif [ -d "$VOLTDB_BIN/../voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib"
    VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"
else
    VOLTDB_LIB="`pwd`/../../../lib"
    VOLTDB_VOLTDB="`pwd`/../../../voltdb"
fi
# make sure YCSB_HOME is set
: ${YCSB_HOME:?"You must set the YCSB_HOME environment variable in order to continue"}
if [ ! -d "$YCSB_HOME" ]; then
    echo "Directory $YCSB_HOME does not exist"; exit;
fi
CLASSPATH=$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
    \ls -1 "$YCSB_HOME"/core/lib/*.jar; \
    \ls -1 "$YCSB_HOME"/core/target/*.jar; \
} 2> /dev/null | paste -sd ':' - )
VOLTDB="$VOLTDB_BIN/voltdb"
VOLTCOMPILER="$VOLTDB_BIN/voltcompiler"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar $CLIENTNAME.jar voltdbroot voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj \
        src/com/yahoo/ycsb/db/*.java \
        src/com/procedures/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
    $VOLTDB compile --classpath obj -o $APPNAME.jar ycsb_ddl.sql
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

function makeclient() {
    srccompile
    jar cf $CLIENTNAME.jar -C obj com/yahoo/ycsb/db
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create -d deployment.xml -l $LICENSE -H $HOST $APPNAME.jar
}

# run the client that drives the example
function workload() {
    # if a client doesn't exist, build one
    if [ ! -f $CLIENTNAME.jar ]; then makeclient; fi
    # run the YCSB workload, which must exist at $YCSB_HOME/workloads
    java -cp "$CLASSPATH:$CLIENTNAME.jar" com.yahoo.ycsb.Client -t -s -db com.yahoo.ycsb.db.VoltClient4 \
        -P $YCSB_HOME/workloads/$WORKLOAD -P workload.properties -P base.properties
}

function load() {
    # if a client doesn't exist, build one
    if [ ! -f $CLIENTNAME.jar ]; then makeclient; fi
    # run the YCSB load phase
    java -cp "$CLASSPATH:$CLIENTNAME.jar" com.yahoo.ycsb.Client -load -s -db com.yahoo.ycsb.db.VoltClient4 \
        -P load.properties -P base.properties
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|makeclient|server [hostname]|load|workload [file]}"
}

# check if an explicit target was specified
if [ $# != 0 ]; then
    # if "server" or "workload" is specified without a second argument, fill in the default values
    if [ $1 == "server" ]; then
        HOST=${2:-"localhost"}; 
        server;
    elif [ $1 == "workload" ]; then
        WORKLOAD=${2:-"workloadb"}; 
        workload;
    elif [ $# -gt 1 ]; then
        help;
        exit;
    else $1;
    fi
else 
    # no arguments starts the server on "localhost"
    HOST="localhost";
    server;
fi
