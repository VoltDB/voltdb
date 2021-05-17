#!/usr/bin/env bash

#set -o nounset #exit if an unset variable is used
set -o errexit #exit on any single command fail

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    # check if voltdb is found relative to where this script was placed,if it's moved out of
    # test_apps and voltdb is not in the path, this will fail.
    VOLTDB_BIN="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../../bin" && pwd )"
    if [ ! -x "$VOLTDB_BIN/voltdb" ]; then
            echo "Can't find the 'voltdb' executable in the local file hierarchy or path "
            echo "Add it to your path to continue"
            exit 1;
    fi
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
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
CLIENTCLASSPATH=osm-import-client.jar:$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb*.jar; \
    \ls -1 lib/*.jar; \
} 2> /dev/null | paste -sd ':' - )
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove binaries, logs, runtime artifacts, etc... but keep the jars
function clean() {
    rm -rf debugoutput voltdbroot log catalog-report.html \
         statement-plans procedures/osmimport/*.class \
         client/osmimport/*.class \
         osmimport-client.jar \
         osmimport-procedures.jar
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    # javac -target 1.7 -source 1.7 -classpath $APPCLASSPATH procedures/voter/*.java
    javac -target 1.8 -source 1.8 -classpath $CLIENTCLASSPATH client/osmimport/*.java
    # build procedure and client jars
    # jar cf voter-procs.jar -C procedures voter
    jar cf osm-import-client.jar -C client osmimport
    # remove compiled .class files
    #rm -rf procedures/voter/*.class client/voter/*.class
    #rm -rf client/voter/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e osm-import-client.jar ]; then
        jars;
    fi
}

# Start DB, load schema, procedures, and static data
function init() {
    echo "starting server in background..."
    voltdb init -l $LICENSE > nohup.log 2>&1
    voltdb start -B -H $HOST >> nohup.log 2>&1
    wait_for_startup
    jars-ifneeded
    sqlcmd < ddl.sql
    #csvloader -f advertisers.csv advertisers
}

# wait for backgrounded server to start up
function wait_for_startup() {
    until echo "exec @SystemInformation, OVERVIEW;" | sqlcmd > /dev/null 2>&1
    do
        sleep 2
        echo " ... Waiting for VoltDB to start"
        if [[ $SECONDS -gt 60 ]]
        then
            echo "Exiting.  VoltDB did not startup within 60 seconds" 1>&2; exit 1;
        fi
    done
}

# run the client that drives the example
function client() {
    jars-ifneeded
    java -classpath $CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
    osmimport.OSMImport --file=monaco.osm  --server=localhost
}


function demo() {


    # start database and load static data
    init

    # Run the demo app
    client

    echo
    echo When you are done with the demo database, \
        remember to use \"voltadmin shutdown\" to stop \
        the server process.
}

function help() {
    echo "Usage: ./run.sh {demo|clean|init|client|demo}"
}

# Run the targets pass on the command line
# If no first arg, run demo
if [ $# -eq 0 ]; then demo; exit; fi
for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
