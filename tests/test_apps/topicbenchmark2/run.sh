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
LOG4J="./log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"
CONFLUENT_HOME=${CONFLUENT_HOME:-/home/opt/confluent-6.0.1}

# NOTE: this tool requires an accessible Confluent distribution download
# and the variable CONFLUENT_HOME set to the location of this distribution, e.g. <path>/confluent-6.0.0.
# This tool has been tested with confluent-6.6.0 and some adjustments to the jar files below may be
# necessary if working with a different Confluent distribution.
CLIENTLIBS=$({ \
    \ls -1 "$VOLTDB_LIB"/jackson-annotations-*.jar; \
    \ls -1 "$VOLTDB_LIB"/jackson-core-*.jar; \
    \ls -1 "$VOLTDB_LIB"/jackson-databind-*.jar; \
    \ls -1 "$VOLTDB_LIB"/jackson-dataformat-cbor-*.jar; \
    \ls -1 "$VOLTDB_LIB"/avro-*.jar; \
    \ls -1 "$VOLTDB_LIB"/kafka-clients-*.jar; \
    \ls -1 "$VOLTDB_LIB"/slf4j-*.jar; \
    \ls -1 "$VOLTDB_LIB"/log4j-*.jar; \
    \ls -1 "$VOLTDB_LIB"/commons-lang3-*.jar; \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
} 2> /dev/null | paste -sd ':' - )
CLIENTCLASSPATH=$CLIENTLIBS:$CLIENTCLASSPATH
CLIENTCLASSPATH=$CONFLUENT_HOME/share/java/schema-registry/*:$CLIENTCLASSPATH
CLIENTCLASSPATH=$CONFLUENT_HOME/share/java/kafka-serde-tools/*:$CLIENTCLASSPATH
CLIENTCLASSPATH=$CONFLUENT_HOME/share/java/confluent-security/schema-registry/*:$CLIENTCLASSPATH

# remove build artifacts
function clean() {
    rm -rf obj debugoutput voltdbroot statement-plans catalog-report.html log *.jar *.csv
    find . -name '*.class' | xargs rm -f
    rm -rf voltdbroot
    rm -rf node0/* node1/* node2/*
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
    TOPICSPORT=9092
    server_common
}

# run the voltdb server locally for AVRO testing
function server_avro() {
    srccompile-ifneeded
    voltdb init --force --config=deployment_avro.xml
    TOPICSPORT=9095
    server_common
}

# run the voltdb server locally for AVRO testing in 3nk1 config
# Confluent platform must be running with schema registry on default port
# Schema registry must be configured to disable schema compatibility:
#   curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \\n  --data '{"compatibility": "NONE"}' \\n  http://localhost:8081/config\n
# Use this sequence of commands:
#   ./run.sh clean
#   ./run.sh server_avro_3nk1
#     (no need for CTRL-C + bg)
#   ./run.sh init_avro
#   ./run.sh run_avro_3nk1_benchmark
#   voltadmin shutdown
#   (stop & cleanup the confluent environment)
function server_avro_3nk1() {
    srccompile-ifneeded
    rm -rf node0/* node1/* node2/*
    voltdb init --force -D node0 -C deployment_avro_3nk1.xml
    voltdb init --force -D node1 -C deployment_avro_3nk1.xml
    voltdb init --force -D node2 -C deployment_avro_3nk1.xml
    sleep 3
    voltdb start --host=localhost:3021,localhost:3022,localhost:3023  -D node0 \
               --admin=21211 --client=21212 --http=8085 --internal=3021 --zookeeper=7181 --topicsport=9093 &
    voltdb start --host=localhost:3021,localhost:3022,localhost:3023 -D node1 \
               --admin=21210 --client=21213 --http=8086 --internal=3022 --zookeeper=7182  --topicsport=9094 &
    voltdb start --host=localhost:3021,localhost:3022,localhost:3023 -D node2 \
              --admin=21209 --client=21214 --http=8087 --internal=3023 --zookeeper=7183  --topicsport=9095 &
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
    VOLTDB_OPTS="${VOLTDB_OPTS}" ${VOLTDB} start -H $HOST -l ${LICENSE} --topicsport=${TOPICSPORT}
}

# start voltdb impersonating kafka for kafka_imports test case
# see tests/test_apps/priority/README_kafka_imports.md
function server_kafka_imports() {
    srccompile-ifneeded
    rm -rf node2/*
    voltdb init --force -D node2 -C deployment_kafka_imports.xml
    sleep 3
    voltdb start --host=localhost:3023 -D node2 \
              --admin=21209 --client=21214 --http=8087 --internal=3023 --zookeeper=7183  --topicsport=9095 &
}

# init kafka_imports test case
function init_kafka_imports() {
  srccompile-ifneeded
  sqlcmd --port=21214 < topicTable.sql
}

# run kafka_imports test case - multiple producer-only clients
function run_kafka_imports() {
    srccompile-ifneeded

    run_kafka_imports_common TEST_TOPIC
    run_kafka_imports_common TEST_TOPIC01
    run_kafka_imports_common TEST_TOPIC02
    run_kafka_imports_common TEST_TOPIC03
    run_kafka_imports_common TEST_TOPIC04
    run_kafka_imports_common TEST_TOPIC05
}

function run_kafka_imports_common() {
    java -classpath topicbenchmark2-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:$LOG4J \
        topicbenchmark2.TopicBenchmark2 \
        --servers=localhost:21214 \
        --topic=$1 \
        --count=50000000 \
        --producers=1 \
        --groups=0 &
}

# load schema and procedures
function init() {
    srccompile-ifneeded
    sqlcmd < topicTable.sql
}

# load schema and procedures for AVRO testing
function init_avro() {
    srccompile-ifneeded
    sqlcmd < topicAvroTable.sql
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
        --count=500000 \
        --producers=2 \
        --groups=2 \
        --groupmembers=10 \
        --pollprogress=10000 \
        --transientmembers=3
}

# producer-only, run once, make sure the (count * producers) matches the count of subscriber-only runs
# note the use of insertrate to avoid timing out producers.
# In case the client complains of batch timeouts you may limit the insertion rate, e.g.:
# --insertrate=10000 \
function run_producers() {
    srccompile-ifneeded
    java -classpath topicbenchmark2-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:$LOG4J \
        topicbenchmark2.TopicBenchmark2 \
        --servers=localhost \
        --topic=TEST_TOPIC \
        --count=5000000 \
        --producers=2 \
        --groups=0
}

# subscriber-only, run once or more, make sure the count matches (count * producers) of the producer-only run
# when repeating the test, make sure to change the group prefix so as to poll the topic from the beginning
function run_subscribers() {
    srccompile-ifneeded
    java -classpath topicbenchmark2-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:$LOG4J \
        topicbenchmark2.TopicBenchmark2 \
        --servers=localhost \
        --topic=TEST_TOPIC \
        --count=10000000 \
        --producers=0 \
        --groups=6 \
        --groupmembers=10 \
        --pollprogress=100000 \
        --transientmembers=3
}

# Use this to benchmark Volt inline avro performance against kafka
function run_avro_benchmark() {
    srccompile-ifneeded
    java -classpath topicbenchmark2-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
        topicbenchmark2.TopicBenchmark2 \
        --servers=localhost \
        --topicPort=9095 \
        --count=500000 \
        --insertrate=10000 \
        --useavro=true \
        --producers=2 \
        --groups=2 \
        --groupmembers=10 \
        --pollprogress=10000 \
        --transientmembers=3
}

# Benchmark case to match ENG-21510
# Note servers localhost only, not identified by ports so targeting the default 21212
function run_avro_3nk1_benchmark() {
    srccompile-ifneeded
    java -classpath topicbenchmark2-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
        topicbenchmark2.TopicBenchmark2 \
        --servers=localhost \
        --count=1000000 \
        --insertrate=10000 \
        --useavro=true \
        --producers=6 \
        --groups=6 \
        --groupmembers=8 \
        --pollprogress=1000000 \
        --transientmembers=3 \
        --maxpollsilence=240 \
        --staticmembers=true
}

# Use this to benchmark Volt inline avro performance against kafka
# In case the client complains of batch timeouts you may limit the insertion rate, e.g.:
# --insertrate=10000 \
function run_avro_producers() {
    srccompile-ifneeded
    java -classpath topicbenchmark2-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:$LOG4J \
        topicbenchmark2.TopicBenchmark2 \
        --servers=localhost \
        --topicPort=9095 \
        --topic=TEST_TOPIC \
        --count=5000000 \
        --useavro=true \
        --producers=2 \
        --groups=0
}

# Use this to benchmark Volt inline avro performance against kafka
function run_avro_subscribers() {
    srccompile-ifneeded
    java -classpath topicbenchmark2-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:$LOG4J \
        topicbenchmark2.TopicBenchmark2 \
        --servers=localhost \
        --topicPort=9095 \
        --topic=TEST_TOPIC \
        --count=10000000 \
        --producers=0 \
        --useavro=true \
        --groups=1 \
        --groupmembers=1 \
        --pollprogress=100000
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
        --verification=random
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
        --verification=random
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
