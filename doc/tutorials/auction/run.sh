#!/usr/bin/env bash

VOLTJAR=`ls ../../../voltdb/voltdb-2.*.jar`
CLASSPATH="../../../voltdb/$VOLTJAR:../../../lib"
VOLTDB="../../../bin/voltdb"
VOLTCOMPILER="../../../bin/voltcompiler"
LICENSE="../../../voltdb/license.xml"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput auction.jar voltdbroot plannerlog.txt voltdbroot
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
    $VOLTCOMPILER obj project.xml auction.jar
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f helloworld.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog auction.jar deployment deployment.xml \
        license $LICENSE leader localhost
}

# run the client that drives the example
function client() {
    srccompile
    java -classpath obj:$CLASSPATH com.auctionexample.Client
}

function export() {
    $EXPORTTOFILE -classpath $CLASSPATH \
        --connect admin --servers localhost --type csv \
        --nonce EXPORTDEMO --user voltdb --password demo
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then echo "Too many arguments to script"; exit; fi
if [ $# = 1 ]; then $1; else server; fi
