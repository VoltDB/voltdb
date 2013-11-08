#!/usr/bin/env bash

APPNAME="adhocbenchmark"

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

CLASSPATH=$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
VOLTDB="$VOLTDB_BIN/voltdb"
VOLTCOMPILER="$VOLTDB_BIN/voltcompiler"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# CentOS/Red Hat have a very old python as the default
# look for 2.6 or 2.7 explicitly
PYTHON=$(which python2.7)
test -z "$PYTHON" && PYTHON=python2.6
GENERATE="$PYTHON scripts/generate.py"

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
    $VOLTDB compile --classpath obj -o $APPNAME.jar -p project.xml
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create -d deployment.xml -l $LICENSE -H localhost $APPNAME.jar
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
        --configfile=cachefriendlyconfig.xml \
        --warmup=5 \
        --duration=60 \
        --test=$1 \
        --querythrottle=30
                           ## \
##        --querytracefile=$1.queries.out
#    echo Sample queries:
#    head -6 $1.queries.out
}

function benchmark-joins() {
    _benchmark join
}

function benchmark-star-joins() {
    _benchmark joinstar
}

function benchmark-projections() {
    _benchmark projection
}

function benchmark-SP-joins() {
    _benchmark joinsp
}

function benchmark-SP-star-joins() {
    _benchmark joinstarsp
}

function benchmark-SP-projections() {
    _benchmark projectionsp
}

function benchmark-MP-joins() {
    _benchmark joinmp
}

function benchmark-MP-star-joins() {
    _benchmark joinstarmp
}

function benchmark-MP-projections() {
    _benchmark projectionmp
}

function benchmark() {
    benchmark-joins
    benchmark-projections
    benchmark-star-joins
    benchmark-SP-joins
    benchmark-SP-projections
    benchmark-SP-star-joins
    benchmark-MP-joins
    benchmark-MP-projections
    # broken by planner bug for now
    # benchmark-MP-star-joins
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|server|benchmark|benchmark-joins|benchmark-projections|benchmark-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
