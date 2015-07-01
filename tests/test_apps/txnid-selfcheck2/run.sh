#!/usr/bin/env bash

APPNAME="txnid"

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

CLASSPATH=$(ls -x "$VOLTDB_VOLTDB"/voltdb-*.jar | tr '[:space:]' ':')$(ls -x "$VOLTDB_LIB"/*.jar | egrep -v 'voltdb[a-z0-9.-]+\.jar' | tr '[:space:]' ':')
VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
CLIENTLOG4J="$VOLTDB_VOLTDB/../tests/log4j-allconsole.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar $APPNAME-alt.jar $APPNAME-noexport.jar voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -target 1.7 -source 1.7 -classpath $CLASSPATH -d obj \
        src/txnIdSelfCheck/*.java \
        src/txnIdSelfCheck/procedures/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile

    # primary catalog
    $VOLTDB compile --classpath obj -o $APPNAME.jar src/txnIdSelfCheck/ddl.sql
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi

    # alternate catalog that adds an index
    $VOLTDB compile --classpath obj -o $APPNAME-alt.jar \
        src/txnIdSelfCheck/ddl.sql src/txnIdSelfCheck/ddl-annex.sql
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi

    # primary no-export catalog
    $VOLTDB compile --classpath obj -o $APPNAME-noexport.jar src/txnIdSelfCheck/ddl-noexport.sql
    # stop if compilation fails
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
function client() {
    benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj txnIdSelfCheck.Benchmark --help
}

function benchmark() {
    srccompile
    java -ea -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$CLIENTLOG4J \
        txnIdSelfCheck.Benchmark \
        --displayinterval=1 \
        --duration=120 \
        --servers=localhost \
        --threads=20 \
        --threadoffset=0 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --entropy=127 \
        --fillerrowsize=10240 \
        --replfillerrowmb=32 \
        --partfillerrowmb=128 \
        --progresstimeout=120 \
        --usecompression=false \
        --allowinprocadhoc=false
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|server|async-benchmark|aysnc-benchmark-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
