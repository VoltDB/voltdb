#!/usr/bin/env bash

APPNAME="topicbenchmark2"
COUNT=10000

# Large memory for running client on performance systems e.g. volt16a
# see volt16a_ functions below
VOLT16A_MEM="-Xms64g -Xmx100g"

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

# add kafka to client classpath
CLIENTLIBS=$({ \
    \ls -1 "$VOLTDB_LIB"/kafka-clients-*.jar; \
    \ls -1 "$VOLTDB_LIB"/slf4j-*.jar; \
    \ls -1 "$VOLTDB_LIB"/log4j-*.jar; \
    \ls -1 "$VOLTDB_LIB"/commons-lang3-*.jar; \
} 2> /dev/null | paste -sd ':' - )
CLIENTCLASSPATH=$CLIENTLIBS:$CLIENTCLASSPATH

# remove build artifacts
function clean() {
    rm -rf obj debugoutput voltdbroot statement-plans catalog-report.html log *.jar *.csv
    find . -name '*.class' | xargs rm -f
    rm -rf voltdbroot
}

# Grab the necessary command line arguments
function parse_command_line() {
    OPTIND=1
    # Return the function to run
    shift $(($OPTIND - 1))
    RUN=$@
}

# compile the source code for procedures and the client into jarfiles
function srccompile() {
    echo
    echo "CLIENTCLASSPATH=\"${CLIENTCLASSPATH}\""
    echo
    javac -classpath $CLIENTCLASSPATH client/topicbenchmark2/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    jar cf topicbenchmark2-client.jar -C client topicbenchmark2
}

function jars() {
     srccompile-ifneeded
}

# compile the procedure and client jarfiles if they don't exist
function srccompile-ifneeded() {
    if [ ! -e topicbenchmark2-client.jar ] ; then
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
    echo "LOG4J=\"${LOG4J}\""
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
    java -classpath topicbenchmark2-client.jar:$CLIENTCLASSPATH topicbenchmark2.TopicBenchmark2 --help
}

# quick test run on default topic
function run_benchmark() {
    srccompile-ifneeded
    java -classpath topicbenchmark2-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
        topicbenchmark2.TopicBenchmark2 \
        --servers=localhost \
        --count=5000 \
        --insertrate=1000 \
        --producers=2 \
        --groups=2 \
        --pollprogress=100 \
        --groupmembers=2
}

# producer-only, run once, make sure the (count * producers) matches the count of subscriber-only runs
# note the use of insertrate to avoid timing out producers
function run_producers() {
    srccompile-ifneeded
    java -classpath topicbenchmark2-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:$LOG4J \
        topicbenchmark2.TopicBenchmark2 \
        --servers=localhost \
        --topic=TEST_TOPIC \
        --count=500000 \
        --insertrate=10000 \
        --producers=2 \
        --groups=0
}

# subscriber-only, run once or more, make sure the count matches (count * producers) of the producer-only run
function run_subscribers() {
    srccompile-ifneeded
    java -classpath topicbenchmark2-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:$LOG4J \
        topicbenchmark2.TopicBenchmark2 \
        --servers=localhost \
        --topic=TEST_TOPIC \
        --count=1000000 \
        --producers=0 \
        --groups=2 \
        --groupmembers=2
}

# Large producer test case successfully tested on volt16a with 3-node cluster
# Note the large memory and java 11
function volt16a_producers() {
    srccompile-ifneeded
    export JAVA_HOME=/opt/jdk-11.0.2
    /opt/jdk-11.0.2/bin/java ${VOLT16A_MEM} -classpath topicbenchmark2-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:$LOG4J \
        topicbenchmark2.TopicBenchmark2 \
        --servers=volt16b,volt16c,volt16d \
        --topic=TEST_TOPIC \
        --count=10000000 \
        --insertrate=1000000 \
        --producers=100 \
        --groups=0
}

# Large producer test case successfully tested on volt16a with 3-node cluster
# Note the large memory and java 11
function volt16a_subscribers() {
    srccompile-ifneeded
    export JAVA_HOME=/opt/jdk-11.0.2
    /opt/jdk-11.0.2/bin/java ${VOLT16A_MEM} -classpath topicbenchmark2-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:$LOG4J \
        topicbenchmark2.TopicBenchmark2 \
        --servers=volt16b,volt16c,volt16d \
        --topic=TEST_TOPIC \
        --count=1000000000 \
        --producers=0 \
        --groups=6 \
        --groupmembers=10 \
        --groupprefix=test6group10members01 \
        --pollprogress=1000000 \
        --sessiontimeout=45 \
        --randomverify=true
}

# Large producer/consumer test case successfully tested on volt16a with 3-node cluster
# Note the large memory and java 11
function volt16a_benchmark() {
    srccompile-ifneeded
    export JAVA_HOME=/opt/jdk-11.0.2
    /opt/jdk-11.0.2/bin/java ${VOLT16A_MEM} -classpath topicbenchmark2-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:$LOG4J \
        topicbenchmark2.TopicBenchmark2 \
        --servers=volt16b,volt16c,volt16d \
        --count=20000000 \
        --insertrate=1000000 \
        --producers=50 \
        --groups=6 \
        --groupmembers=8 \
        --groupprefix=test6group8members01 \
        --pollprogress=1000000 \
        --sessiontimeout=45 \
        --randomverify=true
}

function shutdown() {
    voltadmin shutdown
}

function help() {
    echo "Usage: ./run.sh {clean|jars|server|init|run_benchmark_help|shutdown}"
}

parse_command_line $@
echo $RUN
# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ -n "$RUN" ]; then $RUN; else server; fi
