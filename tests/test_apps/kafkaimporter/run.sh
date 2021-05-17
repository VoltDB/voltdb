#!/usr/bin/env bash

#set -o nounset #exit if an unset variable is used
set -o errexit #exit on any single command fail

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(dirname $(dirname $(pwd)))/bin"
    echo "The VoltDB scripts are not in your PATH."
    echo "For ease of use, add the VoltDB bin directory: "
    echo
    echo $VOLTDB_BIN
    echo
    echo "to your PATH."
    echo
fi
# move voltdb commands into path for this script
PATH=$VOLTDB_BIN:$PATH

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
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/kafka*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
CLIENTCLASSPATH=client.jar:$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdbclient-*.jar; \
    \ls -1 "$VOLTDB_LIB"/kafka*.jar; \
    \ls -1 "$VOLTDB_LIB"/slf4j-api-1.6.2.jar; \
} 2> /dev/null | paste -sd ':' - )
# LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove binaries, logs, runtime artifacts, etc... but keep the jars
function clean() {
    rm -rf debugoutput voltdbroot log catalog-report.html \
         statement-plans build/*.class clientbuild/*.class
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    ant clean
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    ant all
    cp formatter.jar $VOLTDB_BASE/bundles
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    rm -rf felix-cache
    if [ ! -e sp.jar ] || [ ! -e client.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
# note -- use something like this to create the Kafka topic, name
# matching the name used in the deployment file:
#   /home/opt/kafka/bin/kafka-topics.sh --zookeeper kafka2:2181 --topic A7_KAFKAEXPORTTABLE2 --partitions 2 --replication-factor 1 --create
function server() {
    jars-ifneeded
    echo "Starting the VoltDB server."
    echo "Remember -- the Kafka topic must exist before launching this test."
    echo "To perform this action manually, use the command lines: "
    echo
    echo "voltdb init -C deployment.xml -l $LICENSE"
    echo "voltdb start -H $HOST"
    echo
    voltdb init -C deployment.xml -l $LICENSE
    voltdb start -H $HOST
}

#kafka importer
function kafka() {
    jars-ifneeded
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command lines: "
    echo
    echo "voltdb init -C deployment-kafka.xml -l $LICENSE"
    echo "voltdb start -H $HOST"
    echo
    voltdb init -C deployment-kafka.xml -l $LICENSE
    voltdb start -H $HOST
}

# load schema and procedures
function init() {
    jars-ifneeded
    sqlcmd < ddl.sql
}

# wait for backgrounded server to start up
function wait_for_startup() {
    until sqlcmd  --query=' exec @SystemInformation, OVERVIEW;' > /dev/null 2>&1
    do
        sleep 2
        echo " ... Waiting for VoltDB to start"
        if [[ $SECONDS -gt 60 ]]
        then
            echo "Exiting.  VoltDB did not startup within 60 seconds" 1>&2; exit 1;
        fi
    done
}

# startup server in background and load schema
function background_server_andload() {
    jars-ifneeded
    # run the server in the background
    voltdb init -C deployment.xml -l $LICENSE > nohup.log 2>&1
    voltdb start -B -H $HOST >> nohup.log 2>&1
    wait_for_startup
    init
}

# run the client that drives the example
function client() {
    async-benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    jars-ifneeded
    java -classpath $CLIENTCLASSPATH kafkaimporter.client.kafkaimporter.KafkaImportBenchmark --help
}

# latencyreport: default is OFF
# ratelimit: must be a reasonable value if lantencyreport is ON
# Disable the comments to get latency report
function async-benchmark() {
    jars-ifneeded
    java -classpath $CLIENTCLASSPATH \
        client.kafkaimporter.KafkaImportBenchmark \
        --displayinterval=5 \
        --duration=180 \
        --alltypes=false \
        --useexport=false \
        --expected_rows=6000000 \
        --servers=localhost
}

# The following two demo functions are used by the Docker package. Don't remove.
# compile the jars for procs and client code
function demo-compile() {
    jars
}

function demo() {
    echo "starting server in background..."
    background_server_andload
    echo "starting client..."
    client

    echo
    echo When you are done with the demo database, \
        remember to use \"voltadmin shutdown\" to stop \
        the server process.
}

function help() {
    echo "Usage: ./run.sh {clean|server|init|demo|client|async-benchmark|aysnc-benchmark-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
