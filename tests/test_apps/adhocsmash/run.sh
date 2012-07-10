#!/usr/bin/env bash

APPNAME="adhocsmash"
CLASSPATH="`ls -x ../../../voltdb/voltdb-*.jar | tr '[:space:]' ':'``ls -x ../../../lib/*.jar | tr '[:space:]' ':'`"
VOLTDB="../../../bin/voltdb"
VOLTCOMPILER="../../../bin/voltcompiler"
LOG4J="`pwd`/../../../voltdb/log4j.xml"
LICENSE="../../../voltdb/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj \
        src/adhocsmash/*.java \
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
    $VOLTCOMPILER obj project.xml $APPNAME.jar
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment deployment.xml \
        license $LICENSE host $HOST
}

# run the client that drives the example
function client() {
    adhoc-smash
}

function adhoc-smash() {
    srccompile
    java -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        adhocsmash.AdhocSmash \
        --displayinterval=5 \
        --duration=120000 \
        --servers=10.10.180.144 \
        --port=21212 \
        --ratelimit=4000
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|server|adhoc-smash}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
