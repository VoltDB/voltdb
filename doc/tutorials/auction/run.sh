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
    rm -rf obj debugoutput $APPNAME.jar voltdbroot voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj/com/auctionexample/datafiles
    cp src/com/auctionexample/datafiles/*.txt obj/com/auctionexample/datafiles/
    javac -target 1.6 -source 1.6 -classpath $APPCLASSPATH -d obj \
        src/com/auctionexample/*.java \
        procedures/com/auctionexample/*.java \
        procedures/com/auctionexample/debug/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
    $VOLTDB compile --classpath obj -o $APPNAME.jar auction-ddl.sql
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment deployment.xml \
        license $LICENSE host localhost
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
    srccompile
    java -classpath obj:$APPCLASSPATH com.auctionexample.Client
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|client|server}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
