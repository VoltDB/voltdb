#!/usr/bin/env bash

APPNAME="priorityclient"

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

LICENSE="$VOLTDB_VOLTDB/license.xml"
if [ ! -e $LICENSE ]; then
    # locate license from pro build
    alt_lice=$(find $VOLTDB_VOLTDB/../../pro -name license.xml)
    [ -n "$alt_lice" ] && LICENSE=$alt_lice
fi

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
    jar cf priorityclient-client.jar -C src client
}

function servercompile() {
    echo
    echo "Compile server CLIENTCLASSPATH=\"${CLIENTCLASSPATH}\""
    echo
    javac -classpath $CLIENTCLASSPATH src/server/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    jar cf priorityclient-server.jar -C src server
}

function jars() {
    clientcompile
    servercompile
}

function clientcompile-ifneeded() {
  if [ ! -e priorityclient-client.jar ] ; then
      clientcompile;
  fi
}

function servercompile-ifneeded() {
  if [ ! -e priorityclient-server.jar ] ; then
      servercompile;
  fi
}

function srccompile-ifneeded() {
  clientcompile-ifneeded
  servercompile-ifneeded
}

# run the voltdb server locally with VoltDB priority
function jars() {
    srccompile-ifneeded
}

# run the voltdb server locally with VoltDB priority
function server_priority() {
    srccompile-ifneeded
    ${VOLTDB} init --force -C deployment_priority.xml -l ${LICENSE}
    server_common
}

# run the voltdb server locally with VoltDB 'pure' priority (maxwait=0)
function server_pure_priority() {
    srccompile-ifneeded
    ${VOLTDB} init --force -C deployment_pure_priority.xml -l ${LICENSE}
    server_common
}

# run the voltdb server locally with NO_PRIORITY
function server_no_priority() {
    srccompile-ifneeded
    ${VOLTDB} init --force -C deployment_no_priority.xml -l ${LICENSE}
    server_common
}

# kafka_imports test case
function server_kafka_imports() {
    srccompile-ifneeded
    voltdb init --force -C deployment_kafka_imports.xml
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
    sqlcmd < table.sql
}

# kafka_imports test case
function init_kafka_imports() {
    srccompile-ifneeded
    sqlcmd < table.sql
    sqlcmd < kafka_imports.sql
}

#############################################################
# 'all' test cases:
# - test cases testing all priorities
# - each test case can be executed against 2 server configurations
#   - server_priority: server running with priorities
#   - server_no_priority: server running without priorities
#############################################################

# Test SPs at all priorities 1..8, Client V2, With Priorities
function spall2wp() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --warmup=5 \
      --checkpoint=50 \
      --duration=60 \
      --delay=1 \
      --clientversion=2 \
      --prioritize=true \
      --singleclient=true \
      --usesps=true \
      --usemps=false \
      --async=true \
      --verify=true \
      --variation=5 \
      --printstats=false \
      --sprates=800,800,800,800,800,800,800,800 \
      --mprates=-1
}

# Test SPs at all priorities 1..8, Client V2, No Priorities
function spall2np() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --warmup=5 \
      --checkpoint=50 \
      --duration=60 \
      --delay=1 \
      --clientversion=2 \
      --prioritize=false \
      --singleclient=true \
      --usesps=true \
      --usemps=false \
      --async=true \
      --verify=true \
      --variation=5 \
      --printstats=false \
      --sprates=800,800,800,800,800,800,800,800 \
      --mprates=-1
}

# Test SPs at all priorities 1..8, Client V1, With Priorities (note singleclient=false and low variation)
function spall1wp() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --warmup=5 \
      --checkpoint=50 \
      --duration=60 \
      --delay=1 \
      --clientversion=1 \
      --prioritize=true \
      --singleclient=false \
      --usesps=true \
      --usemps=false \
      --async=true \
      --verify=true \
      --variation=2 \
      --printstats=false \
      --sprates=800,800,800,800,800,800,800,800 \
      --mprates=-1
}

# Test SPs at all priorities 1..8, Client V2, No Priorities
function spall1np() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --warmup=5 \
      --checkpoint=50 \
      --duration=60 \
      --delay=1 \
      --clientversion=1 \
      --prioritize=false \
      --singleclient=false \
      --usesps=true \
      --usemps=false \
      --async=true \
      --verify=true \
      --variation=5 \
      --printstats=false \
      --sprates=800,800,800,800,800,800,800,800 \
      --mprates=-1
}

# Test MPs at all priorities 1..8, Client V2, With Priorities
function mpall2wp() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --warmup=5 \
      --checkpoint=50 \
      --duration=60 \
      --delay=1 \
      --clientversion=2 \
      --prioritize=true \
      --singleclient=true \
      --usesps=false \
      --usemps=true \
      --async=true \
      --verify=true \
      --variation=5 \
      --printstats=false \
      --sprates=-1 \
      --mprates=50,50,50,50,50,50,50,50
}

# Test MPs at all priorities 1..8, Client V2, No Priorities
function mpall2np() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --warmup=5 \
      --checkpoint=50 \
      --duration=60 \
      --delay=1 \
      --clientversion=2 \
      --prioritize=false \
      --singleclient=true \
      --usesps=false \
      --usemps=true \
      --async=true \
      --verify=true \
      --variation=5 \
      --printstats=false \
      --sprates=-1 \
      --mprates=50,50,50,50,50,50,50,50
}

# Test MPs at all priorities 1..8, Client V1, With Priorities (note singleclient=false, and verification disabled)
function mpall1wp() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --warmup=5 \
      --checkpoint=50 \
      --duration=60 \
      --delay=1 \
      --clientversion=1 \
      --prioritize=true \
      --singleclient=false \
      --usesps=false \
      --usemps=true \
      --async=true \
      --verify=false \
      --variation=5 \
      --printstats=false \
      --sprates=-1 \
      --mprates=50,50,50,50,50,50,50,50
}

# Test MPs at all priorities 1..8, Client V1, No Priorities
function mpall1np() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --warmup=5 \
      --checkpoint=50 \
      --duration=60 \
      --delay=1 \
      --clientversion=1 \
      --prioritize=false \
      --singleclient=true \
      --usesps=false \
      --usemps=true \
      --async=true \
      --verify=true \
      --variation=5 \
      --printstats=false \
      --sprates=-1 \
      --mprates=50,50,50,50,50,50,50,50
}

# Test SPs and MPs at all priorities 1..8, Client V2, With Priorities, no verification
function xxall2wp() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --warmup=5 \
      --checkpoint=50 \
      --duration=60 \
      --delay=1 \
      --clientversion=2 \
      --prioritize=true \
      --singleclient=true \
      --usesps=true \
      --usemps=true \
      --async=true \
      --verify=false \
      --variation=5 \
      --printstats=false \
      --sprates=800,800,800,800,800,800,800,800 \
      --mprates=20,20,20,20,20,20,20,20
}

# Test SPs and MPs at all priorities 1..8, Client V2, No Priorities
function xxall2np() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --warmup=5 \
      --checkpoint=50 \
      --duration=60 \
      --delay=1 \
      --clientversion=2 \
      --prioritize=false \
      --singleclient=true \
      --usesps=true \
      --usemps=true \
      --async=true \
      --verify=true \
      --variation=5 \
      --printstats=false \
      --sprates=800,800,800,800,800,800,800,800 \
      --mprates=20,20,20,20,20,20,20,20
}

# Test SPs and MPs at all priorities 1..8, Client V1, With Priorities (note singleclient=false and  no verification)
function xxall1wp() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --warmup=5 \
      --checkpoint=50 \
      --duration=60 \
      --delay=1 \
      --clientversion=1 \
      --prioritize=true \
      --singleclient=false \
      --usesps=true \
      --usemps=true \
      --async=true \
      --verify=false \
      --variation=2 \
      --printstats=false \
      --sprates=800,800,800,800,800,800,800,800 \
      --mprates=20,20,20,20,20,20,20,20
}

# Test SPs and MPs at all priorities 1..8, Client V2, No Priorities, no verification
function xxall1np() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --warmup=5 \
      --checkpoint=50 \
      --duration=60 \
      --delay=1 \
      --clientversion=1 \
      --prioritize=false \
      --singleclient=false \
      --usesps=true \
      --usemps=true \
      --async=true \
      --verify=true \
      --variation=5 \
      --printstats=false \
      --sprates=800,800,800,800,800,800,800,800 \
      --mprates=10,10,10,10,10,10,10,10
}

# Test kafka_imports scenario - using V2 client, no priorities
# note also no delay in order to equalize with importer procedures execution times
function run_kafka_imports() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --async=true \
      --sprates=1200,1200,1200,1200,1200,1200,1200,1200 \
      --usemps=false \
      --prioritize=false
}

# test SPs only, with priorities
function spclient() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --async=true \
      --delay=1 \
      --usemps=false
}

# test SPs only, with priorities, 1 client per thread
function spclientxx() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --async=true \
      --singleclient=false \
      --delay=1 \
      --usemps=false
}

# test MPs only, with priorities
function mpclient() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --async=true \
      --delay=0 \
      --usesps=false
}

# test SPs only, no priorities using client V1. Verify that the invoked procedures TestSpInsert01 and TestSpInsert02
# have the default priority specified by the deployment (need IV2QUEUETRACE logging enabled). Verify that this
# default is clipped to lowest priority when the default priority > priority count
function spclientnp() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --async=true \
      --servers=localhost \
      --delay=1 \
      --usemps=false \
      --prioritize=false
}

# Sameas above but 1 client/priority level
function spclientnpxx() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --async=true \
      --singleclient=false \
      --servers=localhost \
      --delay=1 \
      --usemps=false \
      --prioritize=false
}

function mpclientnp() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --async=true \
      --servers=localhost \
      --delay=0 \
      --usesps=false \
      --prioritize=false
}

# test SPs only, with 6 priorities (i.e. 2 above the priority count of 4)
# traffic at priority 2 is the base load, other priorities at 1 per second (except 0 which is omitted)
# verify average times at priority 1 < average times at priority 2, and priorities 3..5 have greater average time than priority 2
# with IV2QUEUETRACE logging enabled verify priorities for TestSpInsert04 and TestSpInsert05 were clipped at 3
function spclient_pc06() {
  srccompile-ifneeded
  java -classpath priorityclient-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file:${LOG4J} \
      client.PriorityClient \
      --servers=localhost \
      --async=true \
      --delay=1 \
      --sprates=-1,1,3000,1,1,1 \
      --usemps=false
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
# If no first arg, run server in no-priority mode
if [ -n "$RUN" ]; then $RUN; else server_no_priority; fi
