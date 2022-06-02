#!/usr/bin/env bash

# leader host for startup purposes only
# (once running, all nodes are the same -- no leaders)
STARTUPLEADERHOST="localhost"
# list of cluster nodes separated by commas in host:[port] format
SERVERS="localhost"

# WEB SERVER variables
WEB_PORT=8081

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

# remove binaries, logs, runtime artifacts, etc... but keep the jars
function clean() {
    rm -rf voltdbroot log procedures/adperformance/*.class client/adperformance/*.class
    rm -f web/http.log web/http.pid
    rm -rf db/obj db/$APPNAME.jar db/nohup.log
    rm -rf client/obj client/log
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    clean
    rm -rf adperformance-procs.jar adperformance-client.jar
}

function webserver() {
    cd web; python -m SimpleHTTPServer $WEB_PORT
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -classpath $APPCLASSPATH procedures/adperformance/*.java
    javac -classpath $CLIENTCLASSPATH client/adperformance/*.java
    # build procedure and client jars
    jar cf adperformance-procs.jar -C procedures adperformance
    jar cf adperformance-client.jar -C client adperformance
    # remove compiled .class files
    rm -rf procedures/adperformance/*.class client/adperformance/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e adperformance-procs.jar ] || [ ! -e adperformance-client.jar ]; then
        jars;
    fi
}

# Init to directory voltdbroot
function voltinit-ifneeded() {
    voltdb init --force
}

# run the voltdb server locally
function server() {
    voltinit-ifneeded
    voltdb start -H $STARTUPLEADERHOST
}

# load schema and procedures
function init() {
    jars-ifneeded
    sqlcmd --servers=$SERVERS < ddl.sql
}

function client() {
    jars-ifneeded
    java -classpath adperformance-client.jar:$CLIENTCLASSPATH \
         adperformance.AdTrackingBenchmark \
         --displayinterval=5 \
         --warmup=5 \
         --duration=120 \
         --servers=$SERVERS \
         --ratelimit=20000 \
         --sites=200 \
         --pagespersite=20 \
         --advertisers=40 \
         --campaignsperadvertiser=10 \
         --creativespercampaign=10
}

function help() {
    echo "Usage: ./run.sh {clean|cleanall|jars|server|init|client|client-help}"
}

# Run the targets pass on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then server; exit; fi
for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
