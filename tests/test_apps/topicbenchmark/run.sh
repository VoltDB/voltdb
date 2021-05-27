#!/usr/bin/env bash

APPNAME="topicbenchmark"
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
    rm -rf voltdbroot
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
    javac -classpath $APPCLASSPATH procedures/topicbenchmark/*.java
    javac -classpath $CLIENTCLASSPATH client/topicbenchmark/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    jar cf topicbenchmark-client.jar -C client topicbenchmark
    jar cf topicbenchmark-procedures.jar -C procedures topicbenchmark
    # add KafkaClientVerifier from genqa
    cd genqa; ./run.sh jars ; cd ../
    jar uf topicbenchmark-client.jar -C genqa/obj genqa
}

function jars() {
     srccompile-ifneeded
}

# compile the procedure and client jarfiles if they don't exist
function srccompile-ifneeded() {
    if [ ! -e topicbenchmark-procedures.jar ] || [ ! -e topicbenchmark-client.jar ] ; then
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
    sqlcmd < topicTable.sql
}

# run the client that drives the example
function client() {
    run_benchmark
}

function run_benchmark_help() {
    srccompile-ifneeded
    java -classpath topicbenchmark-client.jar:$CLIENTCLASSPATH topicbenchmark.TopicBenchmark --help
}

function run_benchmark() {
    srccompile-ifneeded
    java -classpath topicbenchmark-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        topicbenchmark.TopicBenchmark \
        --duration=30 \
        --servers=localhost \
        --count=500000
}

function run_benchmark_10x() {
    srccompile-ifneeded
    java -classpath topicbenchmark-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        topicbenchmark.TopicBenchmark \
        --duration=30 \
	      --multiply=10 \
        --servers=localhost
}

function run_benchmark_100x() {
    srccompile-ifneeded
    java -classpath topicbenchmark-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        topicbenchmark.TopicBenchmark \
        --duration=300 \
	      --multiply=100 \
        --servers=localhost
}

function shutdown() {
    voltadmin shutdown
}

function help() {
    echo "Usage: ./run.sh {clean|jars|server|init|run_benchmark|run_benchmark_10x|run_benchmark_100x|run_benchmark_help|shutdown}"
}

parse_command_line $@
echo $RUN
# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ -n "$RUN" ]; then $RUN; else server; fi
