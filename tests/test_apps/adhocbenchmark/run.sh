#!/usr/bin/env bash

APPNAME="adhocbenchmark"
CLASSPATH="`ls -x ../../../voltdb/voltdb-*.jar | tr '[:space:]' ':'``ls -x ../../../lib/*.jar | tr '[:space:]' ':'`"
VOLTDB="../../../bin/voltdb"
VOLTCOMPILER="../../../bin/voltcompiler"
LOG4J="`pwd`/../../../voltdb/log4j.xml"
LICENSE="../../../voltdb/license.xml"
LEADER="localhost"
GENERATE="python scripts/generate.py"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot voltdbroot log project.xml ddl.sql
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    local srcfiles=`find src -name '*.java'`
    javac -classpath $CLASSPATH -d obj $srcfiles
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    $GENERATE || exit
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
    benchmark
}

# benchmark test
# Use this target for argument help
function benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj ${APPNAME}.Benchmark --help
}

function _benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        ${APPNAME}.Benchmark \
        --displayinterval=5 \
        --servers=localhost \
        --configfile=config.xml \
        --warmup=5 \
        --duration=60 \
        --test=$1
}

function benchmark-joins() {
    _benchmark join
}

function benchmark-projections() {
    _benchmark projection
}

function benchmark() {
    benchmark-joins
    benchmark-projections
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|server|benchmark|benchmark-joins|benchmark-projections|benchmark-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
