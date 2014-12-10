#!/usr/bin/env bash

APPNAME="schemachange"

# Sets env. vars and provides voltdb_daemon_start()
source ../../scripts/run_tools.sh

# run_tools.sh assumes there is no deployment
DEPLOYMENT=deployment.xml

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj \
        src/schemachange/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit 1; fi
}

# build an application catalog
function catalog() {
    srccompile
    $VOLTDB compile --classpath obj -o $APPNAME.jar ddl.sql
    # stop if compilation fails
    if [ $? != 0 ]; then exit 1; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create -d deployment.xml -l $LICENSE -H $HOST $APPNAME.jar
}

# run the client that drives the example
function client() {
    srccompile
    java -ea -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$CLIENTLOG4J \
        schemachange.SchemaChangeClient \
        --servers=localhost \
        --targetrowcount=100000 \
        --duration=1800
}

# quick client run
function quick() {
    srccompile
    java -ea -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$CLIENTLOG4J \
        schemachange.SchemaChangeClient \
        --servers=localhost \
        --targetrowcount=1000 \
        --duration=60
}

# Quick smoke test automatically starts the server and includes forced
# failures to test retry logic.
# The voltdb_daemon_start() function sourced from test_daemon_server.sh
# starts a daemon server, waits for initialization to complete, and sets
# a trap to kill it whenever this script exits.
function smoke() {
    srccompile || exit 1
    catalog || exit 1
    voltdb_daemon_start $APPNAME.jar $HOST deployment.xml $LICENSE || exit 1
    java -ea -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$CLIENTLOG4J \
        schemachange.SchemaChangeClient \
        --servers=localhost \
        --targetrowcount=1000 \
        --duration=180 \
        --retryForcedPercent=20 \
        --retryLimit=10 \
        --retrySleep=2
    if [ $? -eq 0 ]; then
        echo SUCCESS
    else
        echo FAILURE
    fi
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|server|client|quick|smoke}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit 1; fi
if [ $# = 1 ]; then $1; else server; fi
