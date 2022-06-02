#!/usr/bin/env bash

APPNAME="userhitclient"

# find voltdb binaries
if [ -e ../../bin/voltdb ]; then
    # assume this is the examples folder for a kit
    VOLTDB_BIN="$(dirname $(dirname $(pwd)))/bin"
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

VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="./log4j.xml"
HOST="localhost"

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
CLIENTCLASSPATH=./userhit-server.jar:$CLIENTCLASSPATH

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
    jar cf userhit-client.jar -C src client
}

# Note: using APPCLASSPATH instead of CLIENTCLASSPATH
function servercompile() {
    echo
    echo "Compile server APPCLASSPATH=\"${APPCLASSPATH}\""
    echo
    javac -classpath $APPCLASSPATH src/server/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    jar cf userhit-server.jar -C src server
}

function clientcompile-ifneeded() {
  if [ ! -e userhit-client.jar ] ; then
      clientcompile;
  fi
}

function servercompile-ifneeded() {
  if [ ! -e userhit-server.jar ] ; then
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
    VOLTDB_OPTS="${VOLTDB_OPTS}" ${VOLTDB} start -H $HOST
}

# load schema and procedures
function init() {
    srccompile-ifneeded
    sqlcmd --servers=$SERVERS < ddl.sql
}

function client() {
    run-producer
}

function run-producer() {
  srccompile-ifneeded
  java -classpath userhit-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.UserHitClient \
      --servers=localhost
}

function run-client() {
  srccompile-ifneeded
  java -classpath userhit-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.UserHitClient \
      --servers=localhost \
      --produce=false
}
# Help!
function help() {
echo "
Usage: run.sh TARGET

Targets:
    clean
    jars | jars-ifneeded | servercompile | clientcompile
    server | init
    run-producer | run-client

tl;dr:
    ./run.sh server         compiles jars and starts server
    ./run.sh init           loads DDL

    then choose one of:
    ./run-sh run-producer   runs test using Kafka producer to VoltDB topic
    ./run.sh run-client     runs test using VoltDB Client
"
}

parse_command_line $@
if [ -n "$RUN" ]; then
    echo $RUN
    $RUN
else
    help
fi
