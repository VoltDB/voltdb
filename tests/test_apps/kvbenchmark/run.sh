#!/usr/bin/env bash

# these are usually the same, but not in this case
APPNAME="kvbenchmark"
APPDIR="kvbench"

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
    \ls -1 *.jar; \
} 2> /dev/null | paste -sd ':' - )
VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput ${APPNAME}.jar voltdbroot statement-plans catalog-report.html log
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    mkdir -p obj
    # compile java source
    javac -classpath $APPCLASSPATH -d obj \
        src/${APPDIR}/*.java \
        src/${APPDIR}/procedures/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit 1; fi
    # build the jar file
    jar cf ${APPNAME}.jar -C obj ${APPDIR}
    if [ $? != 0 ]; then exit 2; fi
    # remove compiled .class files
    rm -rf obj
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e ${APPNAME}.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    jars-ifneeded
    # only on Oracle jdk...
    if  java -version 2>&1 | grep -q 'Java HotSpot' ; then
        FR_TEMP=/tmp/${USER}/fr
        mkdir -p ${FR_TEMP}
        # Set up flight recorder options
        VOLTDB_OPTS="-XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseTLAB"
        VOLTDB_OPTS="${VOLTDB_OPTS} -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly"
        VOLTDB_OPTS="${VOLTDB_OPTS} -XX:+UnlockCommercialFeatures -XX:+FlightRecorder"
        VOLTDB_OPTS="${VOLTDB_OPTS} -XX:FlightRecorderOptions=maxage=1d,defaultrecording=true,disk=true,repository=${FR_TEMP},threadbuffersize=128k,globalbuffersize=32m"
        VOLTDB_OPTS="${VOLTDB_OPTS} -XX:StartFlightRecording=name=${APPNAME}"
    fi
    # truncate the voltdb log
    [[ -d voltdbroot/log && -w voltdbroot/log ]] && > voltdbroot/log/volt.log
    # run the server
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command line: "
    echo
    echo "export VOLTDB_OPTS=\"${VOLTDB_OPTS}\""
    echo "${VOLTDB} init -C deployment.xml -j ${APPNAME}.jar -s ddl.sql --force"
    echo "${VOLTDB} start -l ${LICENSE} -H ${HOST}"
    echo
    export VOLTDB_OPTS="${VOLTDB_OPTS}"
    ${VOLTDB} init -C deployment.xml -j ${APPNAME}.jar -s ddl.sql --force
    ${VOLTDB} start -l ${LICENSE} -H ${HOST}
}

# initialize the DB by running the DDL file; should be unnecessary if the
# VoltDB server was started using the 'server' function above
function init() {
    jars-ifneeded
    sqlcmd < ddl.sql
}

# run the client that drives the example
function client() {
    sync-benchmark
}

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
function sync-benchmark-help() {
    jars-ifneeded
    java -classpath ${APPCLASSPATH}:${APPNAME}.jar ${APPDIR}.SyncBenchmark --help
}

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
function http-benchmark-help() {
    jars-ifneeded
    java -classpath ${APPCLASSPATH}:${APPNAME}.jar ${APPDIR}.HTTPBenchmark --help
}

function sync-benchmark() {
    jars-ifneeded
    java -classpath ${APPNAME}.jar:$APPCLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        ${APPDIR}.SyncBenchmark \
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

function http-benchmark() {
    jars-ifneeded
    java -classpath ${APPCLASSPATH}:${APPNAME}.jar -Dlog4j.configuration=file://$LOG4J \
        ${APPDIR}.HTTPBenchmark \
        --displayinterval=5 \
        --duration=10 \
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
    echo "Usage: ./run.sh {clean|jars[-ifneeded]|server|client|sync-benchmark[-help]|http-benchmark[-help]}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
