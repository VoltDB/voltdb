#!/usr/bin/env bash

APPNAME="adhocbenchmark"

# find voltdb binaries (customized from examples to be one level deeper)
if [ -e ../../../bin/voltdb ]; then
    # assume this is the examples folder for a kit
    VOLTDB_BIN="$(dirname $(dirname $(dirname $(pwd))))/bin"
elif [ -n "$(which voltdb 2> /dev/null)" ]; then
    # assume we're using voltdb from the path
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    echo "Unable to find VoltDB installation."
    echo "Please add VoltDB's bin directory to your path."
    exit -1
fi

# call script to set up paths, including
# java classpaths and binary paths
source $VOLTDB_BIN/voltenv

# leader host for startup purposes only
# (once running, all nodes are the same -- no leaders)
STARTUPLEADERHOST="localhost"
# list of cluster nodes separated by commas in host:[port] format
SERVERS="localhost"

LOG4J="$VOLTDB_VOLTDB/log4j.xml"

GENERATE="python scripts/generate.py"

# remove build artifacts
function clean() {
    rm -rf $APPNAME-client.jar voltdbroot log ddl.sql src/$APPNAME/*.class
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    $GENERATE
    # compile java source
    javac -classpath $APPCLASSPATH src/$APPNAME/*.java
    # build procedure and client jars
    jar cf $APPNAME-client.jar -C src $APPNAME
    # remove compiled .class files
    rm -rf src/$APPNAME/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e $APPNAME-client.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    # note: "init --force" will delete any existing data
    voltdb init --force
    voltdb start -H $STARTUPLEADERHOST
}

# load schema and procedures
function init() {
    jars-ifneeded
    $GENERATE || exit
    sqlcmd < ddl.sql
}

# run the client that drives the example
function client() {
    benchmark
}

# benchmark test
# Use this target for argument help
function benchmark-help() {
    jars-ifneeded
    java -classpath $APPNAME-client.jar:$APPCLASSPATH ${APPNAME}.Benchmark --help
}

function _benchmark() {
    jars-ifneeded
    java -classpath $APPNAME-client.jar:$APPCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        ${APPNAME}.Benchmark \
        --displayinterval=5 \
        --servers=localhost \
        --configfile=config.xml \
        --warmup=5 \
        --duration=60 \
        --test=$1 \
        --querythrottle=30
## \
## --querytracefile=$1.queries.out
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
    echo "Usage: ./run.sh {clean|jars|server|init|benchmark|benchmark-help}"
}

# Run the targets pass on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then server; exit; fi
for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
