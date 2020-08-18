#!/usr/bin/env bash

APPNAME="exportbenchmark"
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

# call script to set up paths, including
# java classpaths and binary paths
source $VOLTDB_BIN/voltenv

VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput voltdbroot statement-plans catalog-report.html log *.jar *.csv
    find . -name '*.class' | xargs rm -f
    if [ -e ${VOLTDB_LIB}/extension/exportbenchmark-exporter.jar ]; then
      echo
      echo "Removing exportbenchmark-exporter.jar from ${VOLTDB_LIB}/extension"
      echo
      rm -f ${VOLTDB_LIB}/extension/exportbenchmark-exporter.jar
    fi
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

# compile the source code for procedures and the client into jarfiles
function srccompile() {
    javac -classpath $APPCLASSPATH procedures/exportbenchmark/*.java
    javac -classpath $CLIENTCLASSPATH client/exportbenchmark/ExportBenchmark.java
    javac -classpath $APPCLASSPATH server/exportbenchmark/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    jar cf exportbenchmark-client.jar -C client exportbenchmark
    jar cf exportbenchmark-exporter.jar -C server exportbenchmark
    jar cf exportbenchmark-procedures.jar -C procedures exportbenchmark
}

function jars() {
     srccompile-ifneeded
}

# compile the procedure and client jarfiles if they don't exist
function srccompile-ifneeded() {
    if [ ! -e ExportBenchmark.jar ] || [ ! -e exportbenchmark-client.jar ] || [ ! -e exportbenchmark-exporter.jar ]; then
        srccompile;
    fi
}

# run the voltdb server locally
function server() {
    srccompile-ifneeded
    voltdb init --force --config=deployment.xml
    server_common
}

function server_common() {
    echo
    echo "Installing exportbenchmark-exporter.jar to ${VOLTDB_LIB}/extension"
    echo
    cp exportbenchmark-exporter.jar ${VOLTDB_LIB}/extension

    # Set up options
    VOLTDB_OPTS="-XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseTLAB"
    VOLTDB_OPTS="${VOLTDB_OPTS} -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly"
    [[ -d log && -w log ]] && > log/volt.log
    # run the server
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command line: "
    echo
    echo "VOLTDB_OPTS=\"${VOLTDB_OPTS}\" ${VOLTDB} start -H $HOST -l ${LICENSE}"
    echo
    echo "VOLTDB_BIN=\"${VOLTDB_BIN}\""
    echo
    VOLTDB_OPTS="${VOLTDB_OPTS}" ${VOLTDB} start -H $HOST -l ${LICENSE}
}

# load schema and procedures
function init() {
    srccompile-ifneeded
    sqlcmd < exportTable.sql
}

# run the client that drives the example - change function to run other test cases
function client() {
    run_benchmark
}

function run_benchmark_help() {
    srccompile-ifneeded
    java -classpath exportbenchmark-client.jar:$CLIENTCLASSPATH exportbenchmark.ExportBenchmark --help
}

# simple test example
function run_benchmark() {
    srccompile-ifneeded
    java -classpath exportbenchmark-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        exportbenchmark.ExportBenchmark \
        --duration=30 \
        --servers=localhost \
        --statsfile=exportbench.csv
}

# tuple multiply test examples
function run_benchmark_10x() {
    srccompile-ifneeded
    java -classpath exportbenchmark-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        exportbenchmark.ExportBenchmark \
        --duration=30 \
	      --multiply=10 \
        --servers=localhost \
        --statsfile=exportbench.csv
}

function run_benchmark_100x() {
    srccompile-ifneeded
    java -classpath exportbenchmark-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        exportbenchmark.ExportBenchmark \
        --duration=60 \
	      --multiply=100 \
        --servers=localhost \
        --statsfile=exportbench.csv
}

# multi-stream test examples, using --count instead of --duration
function run_benchmark_100x_20() {
    srccompile-ifneeded
    java -classpath exportbenchmark-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        exportbenchmark.ExportBenchmark \
        --count=1000000 \
	      --multiply=100 \
        --streams=20 \
        --servers=localhost \
        --statsfile=exportbench.csv
}

function run_benchmark_10x_50() {
    srccompile-ifneeded
    java -classpath exportbenchmark-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        exportbenchmark.ExportBenchmark \
        --count=1000000 \
	      --multiply=10 \
        --streams=50 \
        --servers=localhost \
        --statsfile=exportbench.csv
}

function shutdown() {
    voltadmin shutdown
}

function help() {
    echo "Usage: ./run.sh {clean|jars|server|server_e3|init|run_benchmark|run_benchmark_10x|run_benchmark_100x|run_benchmark_help|shutdown}"
}

parse_command_line $@
echo $RUN
# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ -n "$RUN" ]; then $RUN; else server; fi
