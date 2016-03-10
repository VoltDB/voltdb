#!/usr/bin/env bash

set -o errexit #exit on any single command fail

# VoltDB variables
APPNAME="nbbo"
HOST=localhost
DEPLOYMENT=deployment.xml

# WEB SERVER variables
WEB_PORT=8081

# CLIENT variables
SERVERS=localhost

# set CLASSPATH
if [ -d "/usr/lib/voltdb" ]; then
    # .deb or .rpm install
    PROC_CLASSPATH="$(ls -1 /usr/lib/voltdb/voltdb-*.jar)"
    CLIENT_CLASSPATH="$(ls -1 /usr/lib/voltdb/voltdbclient-*.jar):"
elif [ -d "$(dirname $(which voltdb))" ]; then
    # tar.gz install
    VOLTDB_HOME=$(dirname $(dirname $(which voltdb)))
    PROC_CLASSPATH="$(ls -1 $VOLTDB_HOME/voltdb/voltdb-*.jar)"
    CLIENT_CLASSPATH="$(ls -1 $VOLTDB_HOME/voltdb/voltdbclient-*.jar):"
else
    echo "VoltDB library not found.  If you installed with the tar.gz file, you need to add the bin directory to your PATH"
    exit
fi

function clean() {
    rm -rf log voltdbroot statement-plans catalog-report.html
    rm -f web/http.log
    rm -rf db/obj db/log
    rm -rf client/obj client/log
}

function help() {
    echo "Usage: ./run.sh {clean|server|client}"
}

###############################################
# Database                                    #
###############################################

function server() {
    echo "server"
    voltdb create -B -d db/$DEPLOYMENT -H $HOST
}

function cluster-server() {
    export DEPLOYMENT=deployment-cluster.xml
    server
}

# wait for backgrounded server to start up
function wait_for_startup() {
    until echo "exec @SystemInformation, OVERVIEW;" | sqlcmd > /dev/null 2>&1
    do
        sleep 2
        echo "  waiting for VoltDB to start..."
        if [[ $SECONDS -gt 60 ]]
        then
            echo "Exiting.  VoltDB did not startup within 60 seconds" 1>&2; exit 1;
        fi
    done
}

function compile_procedures() {
    echo "compile_procedures"
    mkdir -p db/obj
    SRC=`find db/src -name "*.java"`
    if [ ! -z "$SRC" ]; then
        javac -classpath $PROC_CLASSPATH -d db/obj $SRC
    fi
    jar cf db/procedures.jar -C db/obj .
}

function init() {
    echo "init"
    if [ ! -e db/procedures.jar ]; then
        compile_procedures;
    fi
    cd db
    sqlcmd < ddl.sql
    cd ..
}

###############################################
# Client                                      #
###############################################

function compile_client() {
    echo "compile_client"
    #CLASSPATH=`ls -1 $VOLTDB_HOME/voltdb/voltdbclient-*.jar`
    #CLASSPATH="$CLASSPATH:"

    pushd client
    # compile client
    mkdir -p obj
    SRC=`find src -name "*.java"`
    javac -Xlint:unchecked -classpath $CLIENT_CLASSPATH -d obj $SRC
    jar cf client.jar -C obj .
    rm -rf obj
    popd
}
function client() {
    echo "client"
    if [ ! -e client/client.jar ]; then
        compile_client;
    fi

    cd client

    echo "running sync benchmark test..."
    java -classpath client.jar:$CLIENT_CLASSPATH \
        nbbo.NbboBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=1800 \
        --ratelimit=20000 \
        --autotune=true \
        --latencytarget=3 \
        --servers=$SERVERS

    cd ..
}

###############################################
# Web server                                  #
###############################################
function start_web() {
    stop_web
    cd web
    nohup python -m SimpleHTTPServer $WEB_PORT > http.log 2>&1 &
    cd ..
    echo "started demo http server"
}

function stop_web() {
    WEB_PID=$(ps -ef | grep "SimpleHTTPServer" | grep "$WEB_PORT" | grep python | awk '{print $2}')
    if [[ ! -z "$WEB_PID" ]]; then
        kill $WEB_PID
        echo "stopped demo http server"
    fi
}

###############################################
# Demo                                        #
###############################################
# The following two demo functions are used by the Docker package. Don't remove.
# compile the jars for procs and client code
function demo-compile() {
    compile_client
    compile_procedures
}

function demo() {
    export DEPLOYMENT=deployment-demo.xml
    server
    wait_for_startup
    init
    client
}


# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
