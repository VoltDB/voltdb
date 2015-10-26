#!/usr/bin/env bash

APPNAME="exportbenchmark2"
COUNT=10000

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
\echo client.jar; \
\ls -1 "$VOLTDB_VOLTDB"/voltdbclient-*.jar; \
\ls -1 "$VOLTDB_LIB"/commons-cli-1.2.jar; \
} 2> /dev/null | paste -sd ':' - )
echo "APPCLASSPATH: " $APPCLASSPATH
VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
rm -rf obj debugoutput $APPNAME.jar voltdbroot statement-plans catalog-report.html log "$VOLTDB_LIB/ExportBenchmark2.jar"
}

# Grab the necessary command line arguments
function parse_command_line() {
OPTIND=1

while getopts ":h?e:n:" opt; do
    case "$opt" in
        e)
        ARG=$( echo $OPTARG | tr "," "\n" )
        for e in $ARG; do
            EXPORTS+=("$e")
        done
        ;;
        n)
        COUNT=$OPTARG
        ;;
    esac
done

# Return the function to run
shift $(($OPTIND - 1))
RUN=$@
}

function build_deployment_file() {
exit
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    ant clean
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    ant all
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    rm -rf felix-cache
    if [ ! -e sp.jar ] || [ ! -e client.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    jars-ifneeded
    # if a catalog doesn't exist, build one
    # if [ ! -f $APPNAME.jar ]; then catalog; fi
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
    echo "VOLTDB_OPTS=\"${VOLTDB_OPTS}\" ${VOLTDB} create -d deployment.xml -l ${LICENSE} -H ${HOST}"
    echo
    VOLTDB_OPTS="${VOLTDB_OPTS}" ${VOLTDB} create -d deployment.xml -l ${LICENSE} -H ${HOST}
}

# run the client that drives the example
function client() {
    run_benchmark
}

function run_benchmark_help() {
    java -classpath obj:$APPCLASSPATH:obj exportbenchmark2.ExportBenchmark --help
}

function run_benchmark() {
    java -classpath :$APPCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        exportbenchmark2.client.exportbenchmark.ExportBenchmark \
        --duration=30 \
        --servers=localhost \
	--statsfile=exportbench.csv
}

function init() {
    sqlcmd < ddl.sql
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|server|run-benchmark|run-benchmark-help|...}"
}

parse_command_line $@
echo $RUN
# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ -n "$RUN" ]; then $RUN; else server; fi
