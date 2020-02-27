#!/usr/bin/env bash

APPNAME="schemachange"

# Sets env. vars and provides voltdb_daemon_start()
source ../../scripts/run_tools.sh

# run_tools.sh assumes there is no deployment
DEPLOYMENT=deployment.xml

# remove build artifacts
function clean() {
    rm -rf obj debugoutput ${APPNAME}.jar voltdbroot log
}

# compile the source code for the client into jarfiles
function jars() {
    mkdir -p obj
    # compile java source
    javac -classpath $CLASSPATH -d obj \
        src/${APPNAME}/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit 1; fi
    # build the jar file
    jar cf ${APPNAME}.jar -C obj ${APPNAME}
    if [ $? != 0 ]; then exit 2; fi
    # remove compiled .class files
    rm -rf obj
}

# compile the jar file, if it doesn't exist
function jars-ifneeded() {
    if [ ! -e ${APPNAME}.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    jars-ifneeded
    # truncate the voltdb log
    [[ -d voltdbroot/log && -w voltdbroot/log ]] && > voltdbroot/log/volt.log
    # run the server
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command line: "
    echo
    echo "${VOLTDB} init -C deployment.xml -j ${APPNAME}.jar -s ddl.sql --force"
    echo "${VOLTDB} start -l ${LICENSE} -H ${HOST}"
    echo
    ${VOLTDB} init -C deployment.xml -j ${APPNAME}.jar -s ddl.sql --force
    ${VOLTDB} start -l ${LICENSE} -H ${HOST}
}

# run the client that drives the example
function client() {
    jars-ifneeded
    java -ea -classpath ${CLASSPATH}:${APPNAME}.jar -Dlog4j.configuration=file://$CLIENTLOG4J \
        schemachange.SchemaChangeClient \
        --servers=localhost \
        --targetrowcount=100000 \
        --duration=1800
}

# Automatically starts the server in the background and runs the client
# with the options provided. voltdb_daemon_start() (test_daemon_server.sh)
# sets a trap to kill the server before the script exits.
function _auto_run() {
    local OPTIONS="$@"
    jars || exit 1
    voltdb_daemon_start $APPNAME.jar $HOST deployment.xml $LICENSE || exit 1
    java -ea -classpath ${CLASSPATH}:${APPNAME}.jar -Dlog4j.configuration=file://$CLIENTLOG4J \
        schemachange.SchemaChangeClient --servers=localhost $OPTIONS
    if [ $? -eq 0 ]; then
        echo SUCCESS
    else
        echo FAILURE
    fi
}

# Run automatic test with forced failures to exercise retry logic.
function auto_smoke() {
    _auto_run \
        --targetrowcount=1000 \
        --duration=180 \
        --retryForcedPercent=20 \
        --retryLimit=10 \
        --retrySleep=2
}

# Run automatic quick test.
function auto_quick() {
    _auto_run --targetrowcount=10000 --duration=180
}

function help() {
    echo "Usage: ./run.sh {clean|jars[-ifneeded]|server|client|...}"
    echo "                {...|auto_smoke|auto_quick}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit 1; fi
if [ $# = 1 ]; then $1; else server; fi
