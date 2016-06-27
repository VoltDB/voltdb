#!/usr/bin/env bash

SCRIPTPATH=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

APPNAME="xdcr"

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

CLASSPATH=$(ls -x "$VOLTDB_VOLTDB"/voltdb-*.jar | tr '[:space:]' ':')$(ls -x "$VOLTDB_LIB"/*.jar | egrep -v 'voltdb[a-z0-9.-]+\.jar' | tr '[:space:]' ':')
VOLTDB="$VOLTDB_BIN/voltdb"
CLIENTLOG4J="$VOLTDB_VOLTDB/../tests/log4j-allconsole.xml"
LICENSE="$SCRIPTPATH/license.xml"
[ -f $LICENSE ] || LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar $APPNAME-alt.jar $APPNAME-noexport.jar \
           voltdbroot voltxdcr1 voltxdcr2 \
           log statement-plans catalog-report.html .checkpoint
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj \
        src/xdcrSelfCheck/*.java \
        src/xdcrSelfCheck/procedures/*.java \
        src/xdcrSelfCheck/scenarios/*.java \
        src/xdcrSelfCheck/resolves/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile

    # primary catalog
    $VOLTDB compile --classpath obj -o $APPNAME.jar src/xdcrSelfCheck/ddl.sql
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb xdcr1 locally
function xdcr1() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi

    # run the server
    LOG4J_CONFIG_PATH="$SCRIPTPATH/xdcr1.log4j.xml" \
    $VOLTDB create -d xdcr1.deployment.xml -l $LICENSE -H $HOST $APPNAME.jar \
            --internal=3021 \
            --replication=5555 \
            --zookeeper=7181 \
            --client=21212 \
            --admin=21211 \
            --http=8080
}

# run the voltdb xdcr2 locally
function xdcr2() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi

    # run the server
    LOG4J_CONFIG_PATH="$SCRIPTPATH/xdcr2.log4j.xml" \
    $VOLTDB create -d xdcr2.deployment.xml -l $LICENSE -H $HOST $APPNAME.jar \
            --internal=3022 \
            --replication=5556 \
            --zookeeper=7182 \
            --client=21214 \
            --admin=21213 \
            --http=8082
}

# run the client that drives the example
function client() {
    async-xdcr-bench
}

# plot xdcr conflicts from partitioned table
function pp() {
    ./plot.py partitioned
}

# plot xdcr conflicts from replicated table
function pr() {
    ./plot.py replicated
}

# Benchmark sample
# Use this target for argument help
function benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj xdcrSelfCheck.Benchmark --help
}

function async-xdcr-bench() {
    # srccompile
    java $JAVA_OPTS -ea -classpath txnid.jar:$CLASSPATH:obj -Dlog4j.configuration=file://$CLIENTLOG4J \
        xdcrSelfCheck.Benchmark \
        --displayinterval=1 \
        --duration=40 \
        --primaryservers=localhost:21212 \
        --secondaryservers=localhost:21214 \
        --threads=20 \
        --threadoffset=0 \
        --minvaluesize=32 \
        --maxvaluesize=32 \
        --entropy=127 \
        --progresstimeout=120 \
        --usecompression=false \
        --primaryvoltdbroot="./voltxdcr1" \
        --secondaryvoltdbroot="./voltxdcr2"
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|xdcr1|xdcr2|client|pp|pr|help|benchmark-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
