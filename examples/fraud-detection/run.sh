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
    rm -rf frauddetection-procs.jar frauddetection-client.jar web-node/fraud-detection-web/node_modules data/cards.csv
}

function cleanall() {
    clean
}

function webserver() {
    cd web; python -m SimpleHTTPServer $WEB_PORT
}

function npminstall() {
    cd web-node/fraud-detection-web
    npm install
}

# Start the node server
function nodeserver() {
    cd web-node/fraud-detection-web
    npm start
}

function start_export_web() {
    cd exportWebServer; python exportServer.py
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -g -classpath $APPCLASSPATH procedures/frauddetection/*.java
    javac -g -classpath $APPCLASSPATH client/frauddetection/*.java
    # build procedure and client jars
    jar cf frauddetection-procs.jar -C procedures frauddetection
    jar cf frauddetection-client.jar -C client frauddetection
    # remove compiled .class files
    rm -rf procedures/frauddetection/*.class client/frauddetection/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e frauddetection-procs.jar ] || [ ! -e frauddetection-client.jar ]; then
        jars;
    fi
}

# Init to directory voltdbroot
function voltinit-ifneeded() {
    jars-ifneeded
    voltdb init -C deployment.xml --force
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
    echo "----Loading Redline Stations----"
    csvloader --servers $SERVERS --port 21211 --file data/redline.csv --reportdir log stations
    csvloader --servers $SERVERS --port 21211 --file data/trains.csv --reportdir log trains
    rm -f data/cards.csv
    echo "----Loading Cards----"
    java -classpath frauddetection-client.jar:$APPCLASSPATH frauddetection.CardGenerator \
        --output=data/cards.csv
    csvloader --servers $SERVERS --port 21211 --file data/cards.csv --reportdir log cards
    # Now enable the importers to start fetching the data.
    voltadmin update deployment-enable.xml
}

# run the client that drives the example with some editable options
function train() {
    jars-ifneeded
    java -classpath frauddetection-client.jar:$APPCLASSPATH frauddetection.FraudSimulation \
        --broker=localhost:9092 --count=0
}

# generate metro cards
function generate-cards() {
    jars-ifneeded
    java -classpath frauddetection-client.jar:$APPCLASSPATH frauddetection.CardGenerator \
        --output=data/cards.csv
}

function help() {
    echo "Usage: ./run.sh {cleanall|clean|jars|server|init|train}"
}

# Run the targets pass on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then server; exit; fi
for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
