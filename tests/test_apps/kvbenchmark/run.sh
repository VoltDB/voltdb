#!/usr/bin/env bash

APPNAME="kvbenchmark"

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(dirname $(dirname $(dirname $(pwd))))/bin"
    echo "The VoltDB scripts are not in your PATH."
    echo "For ease of use, add the VoltDB bin directory: "
    echo
    echo $VOLTDB_BIN
    echo
    echo "to your PATH."
    echo
fi
# installation layout has all libraries in $VOLTDB_ROOT/lib/voltdb
if [ -d "$VOLTDB_BIN/../lib/voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib/voltdb"
    VOLTDB_VOLTDB="$VOLTDB_LIB"
# distribution layout has libraries in separate lib and voltdb directories
else
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib"
    VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"
fi

APPCLASSPATH=$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot statement-plans catalog-report.html log
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -target 1.7 -source 1.7 -classpath $APPCLASSPATH -d obj \
        src/kvbench/*.java \
        src/kvbench/procedures/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
    echo "Compiling the kvbenchmark application catalog."
    echo "To perform this action manually, use the command line: "
    echo
    echo "voltdb compile --classpath obj -o $APPNAME.jar ddl.sql"
    echo
    $VOLTDB compile --classpath obj -o $APPNAME.jar ddl.sql
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    FR_TEMP=/tmp/${USER}/fr
    mkdir -p ${FR_TEMP}
    # Set up flight recorder options
    VOLTDB_OPTS="-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseTLAB"
    VOLTDB_OPTS="${VOLTDB_OPTS} -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly"
    VOLTDB_OPTS="${VOLTDB_OPTS} -XX:+UnlockCommercialFeatures -XX:+FlightRecorder"
    VOLTDB_OPTS="${VOLTDB_OPTS} -XX:FlightRecorderOptions=maxage=1d,defaultrecording=true,disk=true,repository=${FR_TEMP},threadbuffersize=128k,globalbuffersize=32m"
    VOLTDB_OPTS="${VOLTDB_OPTS} -XX:StartFlightRecording=name=${APPNAME}"
    # truncate the voltdb log
    [[ -d log && -w log ]] && > log/volt.log
    # run the server
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command line: "
    echo 
    echo "VOLTDB_OPTS=\"${VOLTDB_OPTS}\" ${VOLTDB} create -d deployment.xml -l ${LICENSE} -H ${HOST} ${APPNAME}.jar"
    echo
    VOLTDB_OPTS="${VOLTDB_OPTS}" ${VOLTDB} create -d deployment.xml -l ${LICENSE} -H ${HOST} ${APPNAME}.jar
}

# run the client that drives the example
function client() {
    sync-benchmark
}

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
function sync-benchmark-help() {
    srccompile
    java -classpath obj:$APPCLASSPATH:obj kvbench.SyncBenchmark --help
}

function sync-benchmark() {
    srccompile
    java -classpath obj:$APPCLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        kvbench.SyncBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --usecompression=false \
        --threads=40 \
        --csvfile=periodic.csv.gz
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|server|sync-benchmark|sync-benchmark-help|...}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
