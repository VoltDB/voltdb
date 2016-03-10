#!/usr/bin/env bash

# set -x

# VoltDB variables
APPNAME="metro"
HOST=localhost
DEPLOYMENT=deployment.xml
COMPILE="true"

# WEB SERVER variables
WEB_PORT=8081

# CLIENT variables
SERVERS=localhost

# Get the PID from PIDFILE if we don't have one yet.
PID=`pgrep -f SimpleHTTPServer`
if [[ -z "${PID}" && -e web/http.pid ]]; then
  PID=$(cat web/http.pid);
fi

EXPORTPID=`pgrep -f exportServer.py`
if [[ -z "${EXPORTPID}" && -e web/exporthttp.pid ]]; then
  PID=$(cat web/exporthttp.pid);
fi

# This script assumes voltdb/bin is in your path
VOLTDB_HOME=$(dirname $(dirname "$(which voltdb)"))
CLASSPATH=`ls -1 $VOLTDB_HOME/voltdb/voltdb-*.jar`
CLASSPATH="$CLASSPATH:`ls -1 $VOLTDB_HOME/lib/commons-cli-*.jar`"

LICENSE="$VOLTDB_HOME/voltdb/license.xml"

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


function start_export_web() {
    if [[ -z "${EXPORTPID}" ]]; then
        cd exportWebServer
        nohup python exportServer.py > exporthttp.log 2>&1 &
        echo $! > exporthttp.pid
        cd ..
        echo "started export http server"
    else
        echo "export http server is already running (PID: ${PID})"
    fi
}

function stop_export_web() {
  if [[ -z "${EXPORTPID}" ]]; then
    echo "export http server is not running (missing PID)."
  else
      kill ${EXPORTPID}
      rm exportWebServer/exporthttp.pid
      echo "stopped export http server (PID: ${EXPORTPID})."
  fi
}


# compile any java stored procedures
function compile_procedures() {
    mkdir -p db/obj
    javac -classpath $CLASSPATH -d db/obj db/src/procedures/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    jar cvf db/metro.jar -C db/obj procedures
}

# run the voltdb server locally
function server() {
    nohup_server
    echo "------------------------------------"
    echo "|  Ctrl-C to stop tailing the log  |"
    echo "------------------------------------"
    tail -f db/nohup.log
}

function init() {
    if [ "$COMPILE" == "true" ]; then
        echo "Compiling procedures"
        compile_procedures
    fi
    sqlcmd < db/ddl.sql
}

function nohup_server() {
    # run the server
    nohup voltdb create -d db/$DEPLOYMENT -l $LICENSE -H $HOST > db/nohup.log 2>&1 &
}

function cluster-server() {
    export DEPLOYMENT=deployment-cluster.xml
    server
}

function export-server() {
    export DEPLOYMENT=deployment-export.xml
    server
}

function client() {
    # if the class files don't exist, compile the client
    if [ ! -d client/obj -o $COMPILE == "true" ]; then compile-client; fi

    cd client

    echo "running benchmark..."
    java -classpath obj:$CLASSPATH -Dlog4j.configuration=file://$VOLTDB_HOME/voltdb/log4j.xml \
        benchmark.MetroBenchmark \
        --displayinterval=5 \
        --warmup=0 \
        --duration=300 \
        --servers=$SERVERS \
        --ratelimit=250000 \
        --autotune=false \
        --latencytarget=1 \
        --cardcount=50000 \
        --stationfilename=data/station_weights.csv

    cd ..
}

function compile-client() {
    # compile client
    pushd client
    mkdir -p obj
 
    javac -Xlint:unchecked -classpath $CLASSPATH -d obj src/benchmark/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    # jar cvf client
    popd
}

# compile the client code
function demo-compile() {
    compile-client
}

function demo() {
    echo "compiling sources..."
    demo-compile
    export DEPLOYMENT=deployment-demo.xml
    echo "starting VoltDB server..."
    nohup_server
    sleep 10
    echo "initializing..."
    init
    echo "starting client..."
    client
}

function help() {
    echo "Usage: ./run.sh {start_web|stop_web|start_export_web|stop_export_web|demo|server|export-server|client|init|clean}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run help
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else help; fi
