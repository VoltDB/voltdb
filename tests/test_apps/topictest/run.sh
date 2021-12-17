#!/usr/bin/env bash

APPNAME="topictest"

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
    javac -classpath $CLIENTCLASSPATH client/topictest/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    jar cf topictest-client.jar -C client topictest

    javac -classpath $CLIENTCLASSPATH server/topictest/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    jar cf topictest-server.jar -C server topictest
}

function jars() {
     srccompile-ifneeded
}

# compile the procedure and client jarfiles if they don't exist
function srccompile-ifneeded() {
    if [ ! -e topictest-client.jar ] ; then
        srccompile;
    fi
    if [ ! -e topictest-server.jar ] ; then
        srccompile;
    fi
}

# run 1-node voltdb server locally
function server() {
    srccompile-ifneeded
    voltdb init --force --config=deployment.xml
    server_common
}

# run 3-node, k=1, voltdb server locally
function server_multi() {
    srccompile-ifneeded
    rm -rf node0/* node1/* node2/*
    voltdb init --force -D node0 -C deployment.xml
    voltdb init --force -D node1 -C deployment.xml
    voltdb init --force -D node2 -C deployment.xml
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
    VOLTDB_OPTS="${VOLTDB_OPTS}" ${VOLTDB} start -H $HOST -l ${LICENSE} --topicsport=9095 &
}

# load schema and procedures
function init() {
    srccompile-ifneeded
    sqlcmd < ddl.sql
}

# run the default test case
function client() {
  client_nokey_procedure01
}

# Demo test case: no includeKey, CSV, invoke procedure01 with keys at position 2, keys repeated in values,
# looonnng test, check results on source01
#
# TEST_TOPIC must be deployed as follows
# <topic name="TEST_TOPIC" format="csv" procedure="procedure01">
#  <property name="producer.parameters.includeKey">false</property>
# </topic>
#
function demo() {
  srccompile-ifneeded
  java -classpath topictest-client.jar:$CLIENTCLASSPATH \
      topictest.TopicTest \
      --count=3600 \
      --includekey=false \
      --keyposition=2 \
      --logprogress=2
}


# Default test case: no includeKey, CSV, invoke procedure01 with keys at position 2, keys repeated in values,
# check results on source01
#
# TEST_TOPIC must be deployed as follows
# <topic name="TEST_TOPIC" format="csv" procedure="procedure01">
#  <property name="producer.parameters.includeKey">false</property>
# </topic>
#
function client_nokey_procedure01() {
  srccompile-ifneeded
  java -classpath topictest-client.jar:$CLIENTCLASSPATH \
      topictest.TopicTest \
      --includekey=false \
      --keyposition=2 \
      --logprogress=2
}

# Test case: with includeKey, CSV, invoke procedure01 with keys at position 2, keys omitted in values,
# check results on source01
#
# TEST_TOPIC must be deployed as follows
# <topic name="TEST_TOPIC" format="csv" procedure="procedure01">
#  <property name="producer.parameters.includeKey">true</property>
# </topic>
#
function client_withkey_procedure01() {
  srccompile-ifneeded
  java -classpath topictest-client.jar:$CLIENTCLASSPATH \
      topictest.TopicTest \
      --includekey=true \
      --keyposition=2 \
      --logprogress=2
}

# Test case: no includeKey, AVRO, invoke procedure01 with keys at position 2, keys repeated in values,
# check results on source01
#
# TEST_TOPIC must be deployed as follows
# <topic name="TEST_TOPIC" format="avro" procedure="procedure01">
#  <property name="producer.parameters.includeKey">false</property>
# </topic>
#
function avro_client_nokey_procedure01() {
  srccompile-ifneeded
  java -classpath topictest-client.jar:$CLIENTCLASSPATH \
      topictest.TopicTest \
      --includekey=false \
      --keyposition=2 \
      --useavro=true \
      --logprogress=2
}

# Test case: with includeKey, AVRO, invoke procedure01 with keys at position 2, keys omitted in values,
# check results on source01
#
# TEST_TOPIC must be deployed as follows
# <topic name="TEST_TOPIC" format="avro" procedure="procedure01">
#  <property name="producer.parameters.includeKey">true</property>
# </topic>
#
function avro_client_withkey_procedure01() {
  srccompile-ifneeded
  java -classpath topictest-client.jar:$CLIENTCLASSPATH \
      topictest.TopicTest \
      --includekey=true \
      --keyposition=2 \
      --useavro=true \
      --logprogress=2
}

# Test case: no includeKey, CSV, invoke procedure02 with keys at position 0, keys repeated in values,
# check results on source02
#
# TEST_TOPIC must be deployed as follows
# <topic name="TEST_TOPIC" format="csv" procedure="procedure02">
#  <property name="producer.parameters.includeKey">false</property>
# </topic>
#
function client_nokey_procedure02() {
  srccompile-ifneeded
  java -classpath topictest-client.jar:$CLIENTCLASSPATH \
      topictest.TopicTest \
      --includekey=false \
      --keyposition=0 \
      --logprogress=2
}

# Test case: with includeKey, CSV, invoke procedure02 with keys at position 0, keys omitted in values,
# check results on source02
#
# TEST_TOPIC must be deployed as follows
# <topic name="TEST_TOPIC" format="csv" procedure="procedure02">
#  <property name="producer.parameters.includeKey">true</property>
# </topic>
#
function client_withkey_procedure02() {
  srccompile-ifneeded
  java -classpath topictest-client.jar:$CLIENTCLASSPATH \
      topictest.TopicTest \
      --includekey=true \
      --keyposition=0 \
      --logprogress=2
}

# Test case: no includeKey, AVRO, invoke procedure02 with keys at position 0, keys repeated in values,
# check results on source01
#
# TEST_TOPIC must be deployed as follows
# <topic name="TEST_TOPIC" format="avro" procedure="procedure02">
#  <property name="producer.parameters.includeKey">false</property>
# </topic>
#
function avro_client_nokey_procedure02() {
  srccompile-ifneeded
  java -classpath topictest-client.jar:$CLIENTCLASSPATH \
      topictest.TopicTest \
      --includekey=false \
      --keyposition=0 \
      --useavro=true \
      --logprogress=2
}

# Test case: with includeKey, AVRO, invoke procedure02 with keys at position 0, keys omitted in values,
# check results on source02
#
# TEST_TOPIC must be deployed as follows
# <topic name="TEST_TOPIC" format="avro" procedure="procedure02">
#  <property name="producer.parameters.includeKey">true</property>
# </topic>
#
function avro_client_withkey_procedure02() {
  srccompile-ifneeded
  java -classpath topictest-client.jar:$CLIENTCLASSPATH \
      topictest.TopicTest \
      --includekey=true \
      --keyposition=0 \
      --useavro=true \
      --logprogress=2
}

function client_help() {
    srccompile-ifneeded
    java -classpath topictest-client.jar:$CLIENTCLASSPATH topictest.TopicTest --help
}

function shutdown() {
    voltadmin shutdown
}

function help() {
    echo "Usage: ./run.sh {help|clean|jars|server|init|client|shutdown}"
}

parse_command_line $@
echo $RUN
# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ -n "$RUN" ]; then $RUN; else help; fi
