#!/usr/bin/env bash

# VoltDB variables
APPNAME="bank_offers"
HOST=localhost
DEPLOYMENT=deployment.xml

# WEB SERVER variables
WEB_PORT=8081

# CLIENT variables
SERVERS=localhost

# This script assumes voltdb/bin is in your path
VOLTDB_HOME=$(dirname $(dirname "$(which voltdb)"))

LICENSE="$VOLTDB_HOME/voltdb/license.xml"

# Get the PID from PIDFILE if we don't have one yet.
if [[ -z "${PID}" && -e web/http.pid ]]; then
  PID=$(cat web/http.pid);
fi

# remove non-source files
function clean() {
    rm -rf voltdbroot statement-plans log catalog-report.html
    rm -f web/http.log web/http.pid
    rm -rf db/obj db/$APPNAME.jar db/nohup.log
    rm -rf client/obj client/log
}

function start_web() {
    if [[ -z "${PID}" ]]; then
        cd web
        nohup python -m SimpleHTTPServer $WEB_PORT > http.log 2>&1 &
        echo $! > http.pid
        cd ..
        echo "started http server"
    else
        echo "http server is already running (PID: ${PID})"
    fi
}
function stop_web() {
  if [[ -z "${PID}" ]]; then
    echo "http server is not running (missing PID)."
  else
      kill ${PID}
      rm web/http.pid
      echo "stopped http server (PID: ${PID})."
  fi
}

# compile any java stored procedures
function compile_procedures() {
    mkdir -p db/obj
    CLASSPATH=`ls -1 $VOLTDB_HOME/voltdb/voltdb-*.jar`
    SRC=`find db/src -name "*.java"`
    if [ ! -z "$SRC" ]; then
	javac -classpath $CLASSPATH -d db/obj $SRC
        # stop if compilation fails
        if [ $? != 0 ]; then exit; fi
    fi
}

# build an application catalog
function catalog() {
    compile_procedures
    voltdb compile --classpath db/obj -o db/$APPNAME.jar db/ddl.sql
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    catalog
    nohup_server
    echo "------------------------------------"
    echo "|  Ctrl-C to stop tailing the log  |"
    echo "------------------------------------"
    tail -f db/nohup.log
}

function nohup_server() {
    # if a catalog doesn't exist, build one
    if [ ! -f "db/$APPNAME.jar" ]; then catalog; fi
    # run the server
    nohup voltdb create -d db/$DEPLOYMENT -l $LICENSE -H $HOST db/$APPNAME.jar > db/nohup.log 2>&1 &
}

function cluster_server() {
    export DEPLOYMENT=deployment-cluster.xml
    server
}

# update catalog on a running database
function update() {
    catalog
    voltadmin update $APPNAME.jar deployment.xml
}

function client() {
    compile_client

    CLASSPATH=`ls -1 $VOLTDB_HOME/voltdb/voltdb-*.jar`
    CLASSPATH="$CLASSPATH:`ls -1 $VOLTDB_HOME/lib/commons-cli-*.jar`"

    cd client

    echo "running sync benchmark test..."
    java -classpath obj:$CLASSPATH -Dlog4j.configuration=file://$VOLTDB_HOME/voltdb/log4j.xml \
	client.OfferBenchmark \
	--displayinterval=5 \
	--warmup=5 \
	--duration=300 \
	--ratelimit=20000 \
	--autotune=true \
	--latencytarget=3 \
	--servers=$SERVERS

    cd ..
}

function compile_client() {
    CLASSPATH=`ls -1 $VOLTDB_HOME/voltdb/voltdb-*.jar`
    CLASSPATH="$CLASSPATH:`ls -1 $VOLTDB_HOME/lib/commons-cli-*.jar`"

    pushd client
    # compile client
    mkdir -p obj
    SRC=`find src -name "*.java"`
    javac -Xlint:unchecked -classpath $CLASSPATH -d obj $SRC
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    popd
}

# compile the catalog and client code
function compile() {
    compile_procedures
    compile_client
}

function demo() {
    export DEPLOYMENT=deployment-demo.xml
    nohup_server
    echo "starting client..."
    sleep 10
    client
}


function stop() {
    # stop web server if running
    stop_web
    # stop voltdb if running
    voltadmin shutdown -H $SERVERS
}

function help() {
    echo "Usage: ./run.sh {clean|compile|catalog|start_web|stop_web|server|cluster_server|client}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
