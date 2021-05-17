#!/usr/bin/env bash

APPNAME="deletes"

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

CLASSPATH=$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )


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
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj \
        src/com/*.java \
        src/com/deletes/*.java

    # build procedure and client jars
    jar cf $APPNAME-client.jar -C obj com
    jar cf $APPNAME-procs.jar -C obj com/deletes
    # remove compiled .class files
    rm -rf obj
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
    sqlcmd < deletes-ddl.sql
}

# run the client that drives the example
function client() {
    jars-ifneeded
    java -classpath $APPNAME-client.jar:$CLIENTCLASSPATH $APPNAME.DeletesClient \
        --servers=volt7a \
        --duration=18 \
        --small-strings=true\
        --batches=4
}

function help() {
    echo "Usage: ./run.sh {clean|jars|server|init|client|...}"
}

# Run the targets pass on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then server; exit; fi
for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
