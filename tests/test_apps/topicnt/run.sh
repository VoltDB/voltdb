#!/usr/bin/env bash

APPNAME="topicntclient"

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
HOST="localhost"
CONFLUENT_HOME=${CONFLUENT_HOME:-/home/opt/confluent-6.0.1}

LICENSE="$VOLTDB_VOLTDB/license.xml"
if [ ! -e $LICENSE ]; then
    # locate license from pro build
    alt_lice=$(find $VOLTDB_VOLTDB/../../pro -name license.xml)
    [ -n "$alt_lice" ] && LICENSE=$alt_lice
fi

CLIENTLIBS=$({ \
    \ls -1 "$VOLTDB_LIB"/jackson-annotations-*.jar; \
    \ls -1 "$VOLTDB_LIB"/jackson-core-*.jar; \
    \ls -1 "$VOLTDB_LIB"/jackson-databind-*.jar; \
    \ls -1 "$VOLTDB_LIB"/jackson-dataformat-cbor-*.jar; \
    \ls -1 "$VOLTDB_LIB"/avro-*.jar; \
    \ls -1 "$VOLTDB_LIB"/kafka-clients-*.jar; \
    \ls -1 "$VOLTDB_LIB"/log4j-*.jar; \
    \ls -1 "$VOLTDB_LIB"/slf4j-*.jar; \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
} 2> /dev/null | paste -sd ':' - )
CLIENTCLASSPATH=$CLIENTLIBS:$CLIENTCLASSPATH
CLIENTCLASSPATH=$CONFLUENT_HOME/share/java/schema-registry/*:$CLIENTCLASSPATH
CLIENTCLASSPATH=$CONFLUENT_HOME/share/java/kafka-serde-tools/*:$CLIENTCLASSPATH
CLIENTCLASSPATH=$CONFLUENT_HOME/share/java/confluent-security/schema-registry/*:$CLIENTCLASSPATH
CLIENTCLASSPATH=./topicnt-server.jar:$CLIENTCLASSPATH

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

function clientcompile() {
    echo
    echo "Compile client CLIENTCLASSPATH=\"${CLIENTCLASSPATH}\""
    echo
    javac -classpath $CLIENTCLASSPATH src/client/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    jar cf topicnt-client.jar -C src client
}

# Note: using APPCLASSPATH instead of CLIENTCLASSPATH
function servercompile() {
    echo
    echo "Compile server APPCLASSPATH=\"${APPCLASSPATH}\""
    echo
    javac -classpath $APPCLASSPATH src/server/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    jar cf topicnt-server.jar -C src server
}

function clientcompile-ifneeded() {
  if [ ! -e topicnt-client.jar ] ; then
      clientcompile;
  fi
}

function servercompile-ifneeded() {
  if [ ! -e topicnt-server.jar ] ; then
      servercompile;
  fi
}

# Need to compile server before client
function srccompile-ifneeded() {
  servercompile-ifneeded
  clientcompile-ifneeded
}

# compile
function jars() {
    servercompile
    clientcompile
}

function jars-ifneeded() {
    srccompile-ifneeded
}

function server() {
    srccompile-ifneeded
    voltdb init --force -C deployment.xml
    server_common
}

# Note - flight recording requires J11 on my Mac
function server_common() {
    # Set up options
    VOLTDB_OPTS="-XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseTLAB"
    VOLTDB_OPTS="${VOLTDB_OPTS} -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly"
    # VOLTDB_OPTS="${VOLTDB_OPTS} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
    # VOLTDB_OPTS="${VOLTDB_OPTS} -XX:StartFlightRecording=dumponexit=false"
    [[ -d log && -w log ]] && > log/volt.log
    # run the server
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command line: "
    echo
    echo "VOLTDB_OPTS=\"${VOLTDB_OPTS}\" ${VOLTDB} start -H $HOST"
    echo
    echo "VOLTDB_BIN=\"${VOLTDB_BIN}\""
    echo
    echo "LOG4J=\"${LOG4J}\""
    echo
    VOLTDB_OPTS="${VOLTDB_OPTS}" ${VOLTDB} start -H $HOST &
}

# load schema and procedures
function init() {
    srccompile-ifneeded
    sqlcmd < ddl.sql
}

# default test
function client() {
  testsmall
}

# ENG-22239: invoke compound procedure at scale via ingress topic
function scaleproduce() {
  srccompile-ifneeded
  java -classpath topicnt-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.TopicNTClient \
      --servers=volt14b,volt14e,volt14f \
      --brokers=volt14b:9092,volt14e:9092,volt14f:9092 \
      --duration=120 \
      --users=1000000 \
      --cookies=4 \
      --domains=1000 \
      --urls=10000 \
      --producers=10 \
      --rate=7500
}

# ENG-22239: mimic compound procedure using Client2 API
function scaleclient() {
  srccompile-ifneeded
  java -classpath topicnt-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.TopicNTClient \
      --servers=volt14b,volt14e,volt14f \
      --produce=false \
      --duration=120 \
      --users=1000000 \
      --cookies=4 \
      --domains=1000 \
      --urls=10000 \
      --producers=10 \
      --rate=7500
}

# Multinode - ENG-22115
function multinode() {
  srccompile-ifneeded
  java -classpath topicnt-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.TopicNTClient \
      --brokers=localhost:9093,localhost:9094,localhost:9095 \
      --producers=10
}

# small test case
function testsmall() {
  srccompile-ifneeded
  java -classpath topicnt-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.TopicNTClient \
      --servers=localhost \
      --duration=60 \
      --users=10 \
      --cookies=1 \
      --urls=100 \
      --domains=10 \
      --producers=1 \
      --rate=2
}

# small test case, with fail
function testfail() {
  srccompile-ifneeded
  java -classpath topicnt-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.TopicNTClient \
      --servers=localhost \
      --fail=true \
      --duration=60 \
      --users=10 \
      --cookies=1 \
      --urls=100 \
      --domains=10 \
      --producers=1 \
      --rate=2
}

# rerun small test case without trying to initialize the database
function again() {
  srccompile-ifneeded
  java -classpath topicnt-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.TopicNTClient \
      --servers=localhost \
      --initialize=false \
      --duration=60 \
      --users=10 \
      --cookies=1 \
      --urls=100 \
      --domains=10 \
      --producers=1 \
      --rate=2
}

# Use this test case to exercise failure paths in stored procedures
function testsingle() {
  srccompile-ifneeded
  java -classpath topicnt-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.TopicNTClient \
      --servers=localhost \
      --singletest=true \
      --users=10 \
      --cookies=1 \
      --urls=100
}

# Single test no init
function testsingle2() {
  srccompile-ifneeded
  java -classpath topicnt-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.TopicNTClient \
      --servers=localhost \
      --initialize=false \
      --singletest=true \
      --users=10 \
      --cookies=1 \
      --urls=100
}

# Help!
function help() {
echo "
Usage: run.sh TARGET

Targets:
    clean
    jars | jars-ifneeded | servercompile | clientcompile
    server | init
    client
"
}

parse_command_line $@
if [ -n "$RUN" ]; then
    echo $RUN
    $RUN
else
    help
fi
