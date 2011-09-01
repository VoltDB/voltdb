#!/usr/bin/env bash

VOLTJAR=`ls ../../../voltdb/voltdb-2.*.jar`
CLASSPATH="../../../voltdb/$VOLTJAR:../../../lib"
VOLTDB="../../../bin/voltdb"
VOLTCOMPILER="../../../bin/voltcompiler"
LICENSE="../../../voltdb/license.xml"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput helloworld.jar voltdbroot plannerlog.txt voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj *.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function compile() {
    srccompile
    $VOLTCOMPILER obj project.xml helloworld.jar
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f helloworld.jar ]; then compile; fi
    # run the server
    $VOLTDB create catalog helloworld.jar deployment deployment.xml \
        license $LICENSE leader localhost
}

# run the client that drives the example
function client() {
    srccompile
    java -classpath obj:$CLASSPATH Client
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then echo "Too many arguments to script"; exit; fi
if [ $# = 1 ]; then $1; else server; fi
