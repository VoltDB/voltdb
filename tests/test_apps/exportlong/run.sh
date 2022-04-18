#!/usr/bin/env bash

APPNAME="exportlongclient"

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
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

CLIENTLIBS=$({ \
    \ls -1 "$VOLTDB_LIB"/slf4j-*.jar; \
    \ls -1 "$VOLTDB_LIB"/log4j-*.jar; \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
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
function clientcompile() {
    echo
    echo "Compile client CLIENTCLASSPATH=\"${CLIENTCLASSPATH}\""
    echo
    javac -classpath $CLIENTCLASSPATH src/client/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    jar cf exportlong-client.jar -C src client
}

function clientcompile-ifneeded() {
  if [ ! -e exportlong-client.jar ] ; then
      clientcompile;
  fi
}

function srccompile-ifneeded() {
  clientcompile-ifneeded
}

# run the single-node voltdb server locally
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
    echo "VOLTDB_OPTS=\"${VOLTDB_OPTS}\" ${VOLTDB} start -H $HOST -l ${LICENSE}"
    echo
    echo "VOLTDB_BIN=\"${VOLTDB_BIN}\""
    echo
    VOLTDB_OPTS="${VOLTDB_OPTS}" ${VOLTDB} start -H $HOST -l ${LICENSE} &
}

# Default test: localhost, 10 sources, 2 targets, 120s
function client() {
  srccompile-ifneeded
  java -classpath exportlong-client.jar:$CLIENTCLASSPATH \
      client.ExportLongClient
}

# Generate a large PBD data set to investigate file parsing on restart
# Must export to DISABLED DiscardingExportClient targets
function ENG-21603() {
  srccompile-ifneeded
  java -classpath exportlong-client.jar:$CLIENTCLASSPATH \
    client.ExportLongClient \
    --servers=localhost \
    --sources=500 \
    --targets=2 \
    --rate=250 \
    --duration=0
}

function ENG-21637() {
  srccompile-ifneeded
  java -classpath exportlong-client.jar:$CLIENTCLASSPATH \
    client.ExportLongClient \
    --servers=localhost \
    --sources=150 \
    --targets=4
}

# Test case from volt17a cluster of 6 to volt17c running Oracle
function ENG-21670() {
  srccompile-ifneeded
  java -classpath exportlong-client.jar:$CLIENTCLASSPATH \
    client.ExportLongClient \
    --servers=volt17ac1,volt17ac2,volt17ac3,volt17ac4,volt17ac5,volt17ac6 \
    --sources=450 \
    --targets=4 \
    --rate=100 \
    --duration=0
}

# duration 3600s, rate 1000/s (total 10000/s all streams/targets)
function d3600r1000() {
  srccompile-ifneeded
  java -classpath exportlong-client.jar:$CLIENTCLASSPATH \
      client.ExportLongClient \
      --servers=localhost \
      --duration=3600
}

# duration 0 (infinite), rate 1000/s (total 10000/s all streams/targets)
function d0r1000() {
  srccompile-ifneeded
  java -classpath exportlong-client.jar:$CLIENTCLASSPATH \
      client.ExportLongClient \
      --servers=localhost \
      --duration=0
}

# duration 0 (infinite), rate 500/s (total 5000/s all streams/targets)
function d0r500() {
  srccompile-ifneeded
  java -classpath exportlong-client.jar:$CLIENTCLASSPATH \
      client.ExportLongClient \
      --servers=localhost \
      --duration=0 \
      --rate=500
}

# duration 3600s, rate 500/s (total 5000/s all streams/targets)
function d3600r500() {
  srccompile-ifneeded
  java -classpath exportlong-client.jar:$CLIENTCLASSPATH \
      client.ExportLongClient \
      --servers=localhost \
      --duration=3600 \
      --rate=500
}

# duration 3600s, rate 100/s (total 1000/s all streams/targets)
function d3600r100() {
  srccompile-ifneeded
  java -classpath exportlong-client.jar:$CLIENTCLASSPATH \
      client.ExportLongClient \
      --servers=localhost \
      --duration=3600 \
      --rate=100
}

function shutdown() {
    voltadmin shutdown
}

function help() {
    echo "Usage: ./run.sh {clean|jars|server|init|client|shutdown}"
}

parse_command_line $@
echo $RUN
# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ -n "$RUN" ]; then $RUN; else server; fi
