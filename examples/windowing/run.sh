#!/usr/bin/env bash

APPNAME="windowing"

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
CLIENTCLASSPATH=$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdbclient-*.jar; \
    \ls -1 "$VOLTDB_LIB"/commons-cli-1.2.jar; \
} 2> /dev/null | paste -sd ':' - )
VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf debugoutput log $APPNAME.jar voltdbroot \
           procedures/windowing/*.class \
           client/windowing/*.class
}

# compile the source code for procedures and the client
function srccompile() {
    javac -target 1.7 -source 1.7 -classpath $APPCLASSPATH \
        procedures/windowing/*.java \
        client/windowing/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
    echo "Compiling the voter application catalog."
    echo "To perform this action manually, use the command line: "
    echo
    echo "voltdb compile -o $APPNAME.jar ddl.sql"
    echo
    $VOLTDB compile -o $APPNAME.jar ddl.sql
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command line: "
    echo
    echo "$VOLTDB create -d deployment.xml -l $LICENSE -H $HOST $APPNAME.jar"
    echo
    $VOLTDB create -d deployment.xml -l $LICENSE -H $HOST $APPNAME.jar
}

# Use this target for argument help
function client-help() {
    srccompile
    java -classpath client:$CLIENTCLASSPATH windowing.WindowingApp --help
}

## USAGE FOR CLIENT TARGET ##
# usage: windowing.WindowingApp
#     --deletechunksize <arg>   Maximum number of rows to delete in one
#                               transaction.
#     --deleteyieldtime <arg>   Time to pause between deletes when there was
#                               nothing to delete at last check.
#     --displayinterval <arg>   Interval for performance feedback, in
#                               seconds.
#     --duration <arg>          Duration, in seconds.
#     --historyseconds <arg>    Global maximum history targert. Zero if
#                               using row count target.
#     --inline <arg>            Run deletes in the same transaction as
#                               inserts.
#     --maxrows <arg>           Global maximum row target. Zero if using
#                               history target.
#     --password <arg>          Password for connection.
#     --ratelimit <arg>         Maximum TPS rate for inserts.
#     --servers <arg>           Comma separated list of the form
#                               server[:port] to connect to.
#     --user <arg>              User name for connection.

function client() {
    srccompile
    # Note that in the command below, maxrows and historyseconds can't both be non-zero.
    java -classpath client:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        windowing.WindowingApp \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost:21212 \
        --maxrows=0 \
        --historyseconds=30 \
        --inline=false \
        --deletechunksize=100 \
        --deleteyieldtime=100 \
        --ratelimit=15000
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|server|client|client-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
