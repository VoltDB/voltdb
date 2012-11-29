#!/usr/bin/env bash

APPNAME="json"
CLASSPATH="`ls -x ../../voltdb/voltdb-*.jar | tr '[:space:]' ':'``ls -x ../../lib/*.jar | tr '[:space:]' ':'`"
VOLTDB="../../bin/voltdb"
VOLTCOMPILER="../../bin/voltcompiler"
LOG4J="`pwd`/../../voltdb/log4j.xml"
LICENSE="../../voltdb/license.xml"
LEADER="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -target 1.6 -source 1.6 -classpath $CLASSPATH:gson-2.2.2.jar -d obj \
        src/json/*.java \
        src/json/procedures/*.java
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
        license $LICENSE leader $LEADER
}

# run the client that drives the example
function client() {
    json-client
}

# JSON client sample
# Use this target for argument help
function json-client-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj json.JSONClient --help
}

function json-client() {
    srccompile
    java -classpath obj:$CLASSPATH:obj:gson-2.2.2.jar -Dlog4j.configuration=file://$LOG4J \
        json.JSONClient \
        --duration=10 \
        --servers=localhost:21212
}



function help() {
    echo "Usage: ./run.sh {clean|catalog|server|client}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
