#!/usr/bin/env bash

# find voltdb binaries
if [ -e ../../../bin/voltdb ]; then
    # assume this is the tests/test_apps/compound directory
    VOLTDB_BIN="$(dirname $(dirname $(dirname $(pwd))))/bin"
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

# leader host for startup purposes only
# (once running, all nodes are the same -- no leaders)
STARTUPLEADERHOST="localhost"

# list of cluster nodes separated by commas in host:[port] format
SERVERS="localhost"

# remove binaries, logs, runtime artifacts, etc...
function clean {
    rm -rf voltdbroot voltdb_crash* log
    rm -f compound*.jar procs/compound/*.class client/compound/*.class
}

# compile the source code for procedures and the client into jarfiles
function jars {
    # compile java source
    javac -classpath $APPCLASSPATH procs/compound/*.java
    javac -classpath $CLIENTCLASSPATH client/compound/*.java
    # build procedure and client jars
    jar cf compound-procs.jar -C procs compound
    jar cf compound-client.jar -C client compound
    # remove compiled .class files
    rm -f procs/compound/*.class client/compound/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded {
    if [ ! -e compound-procs.jar ] || [ ! -e compound-client.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server {
    jars-ifneeded
    voltdb init -f -j compound-procs.jar -s ddl.sql
    voltdb start -H $STARTUPLEADERHOST
}

version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
add_open=
if [[ $version == 11.0* ]] || [[ $version == 17.0* ]] ; then
        add_open="--add-opens java.base/sun.nio.ch=ALL-UNNAMED"
fi

# run the client that drives the example
function doclient {
    jars-ifneeded
    sqlcmd <populate.sql
    echo ' '
    java $add_open \
        -classpath compound-client.jar:$CLIENTCLASSPATH compound/CompoundClient \
        --duration=60 \
        --servers=$SERVERS \
        --affinityreport=true \
        --ioreport=true \
        --test=$1
}

function client {
    doclient simple
}

function clientnull {
    doclient null
}

function help {
    echo "
Usage:  ./run.sh target...

Targets:
        help | jars | clean
        server
        client | clientnull

tl;dr:
        ./run.sh server    in one terminal
        ./run.sh client    in another terminal
"
}

# Run the targets passed on the command line

if [ $# -eq 0 ];
then
    help
    exit 0
fi

for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
