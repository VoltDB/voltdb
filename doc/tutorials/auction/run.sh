#!/usr/bin/env bash

APPNAME="auction"

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
else
    VOLTDB_LIB="`pwd`/../../../lib"
    VOLTDB_VOLTDB="`pwd`/../../../voltdb"
fi

APPCLASSPATH=$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
VOLTDB="$VOLTDB_BIN/voltdb"
LICENSE="$VOLTDB_VOLTDB/license.xml"
CSVLOADER="$VOLTDB_BIN/csvloader"

DATAFILES="src/com/auctionexample/datafiles/"

# remove build artifacts
function clean() {
    rm -rf debugoutput $APPNAME-*.jar voltdbroot log csvloader_*.*
}

function jars() {
    # compile java source
    javac -target 1.7 -source 1.7 -classpath $APPCLASSPATH \
        src/com/auctionexample/*.java
    javac -target 1.7 -source 1.7 -classpath $APPCLASSPATH \
        procedures/com/auctionexample/*.java \
        procedures/com/auctionexample/debug/*.java
    # build procedure and client jars
    jar cf $APPNAME-procs.jar -C procedures com
    jar cf $APPNAME-client.jar -C src com
    # remove compiled .class files
    rm -rf procedures/com/auctionexample/*.class procedures/com/auctionexample/debug/*.class src/com/auctionexample/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e $APPNAME-procs.jar ] || [ ! -e $APPNAME-client.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    # run the server
    $VOLTDB create -d deployment.xml -l $LICENSE -H localhost
}

# load schema and procedures
function init() {
    jars-ifneeded
    $VOLTDB_BIN/sqlcmd < auction-ddl.sql
}

# run the client that drives the example
function client() {
    # load the csv files
    $CSVLOADER -f $DATAFILES/items.txt \
            -p InsertIntoItemAndBid \
                --user program \
                --password pass
    $CSVLOADER -f $DATAFILES/categories.txt \
                        -p InsertIntoCategory \
                        --user program \
                        --password pass
    $CSVLOADER -f $DATAFILES/users.txt \
                        -p InsertIntoUser \
                        --user program \
                        --password pass
    jars-ifneeded
    java -classpath $APPCLASSPATH:$APPNAME-client.jar com.auctionexample.Client
}

function help() {
    echo "Usage: ./run.sh {clean|init|client|server}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
