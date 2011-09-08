#!/usr/bin/env bash

APPNAME="auction"
VOLTJAR=`ls ../../../voltdb/voltdb-2.*.jar`
CLASSPATH="$VOLTJAR:../../../lib"
VOLTDB="../../../bin/voltdb"
EXPORTTOFILE="../../../bin/exporttofile"
VOLTCOMPILER="../../../bin/voltcompiler"
LICENSE="../../../voltdb/license.xml"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot plannerlog.txt voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj/com/auctionexample/datafiles
    cp src/com/auctionexample/datafiles/*.txt obj/com/auctionexample/datafiles/
    javac -classpath $CLASSPATH -d obj \
        src/com/auctionexample/*.java \
        procedures/com/auctionexample/*.java \
        procedures/com/auctionexample/debug/*.java
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
        license $LICENSE leader localhost
}

# run the client that drives the example
function client() {
    srccompile
    java -classpath obj:$CLASSPATH com.auctionexample.Client
}

function export() {
    $EXPORTTOFILE \
        --connect client --servers localhost --type csv \
        --nonce EXPORTDEMO --user voltdb --password demo
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|client|server|export}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
