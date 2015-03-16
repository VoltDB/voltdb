#!/usr/bin/env bash

#set -o nounset #exit if an unset variable is used
set -o errexit #exit on any single command fail

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
CLIENTCLASSPATH=ddlwindowing-client.jar:$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdbclient-*.jar; \
    \ls -1 "$VOLTDB_LIB"/commons-cli-1.2.jar; \
} 2> /dev/null | paste -sd ':' - )
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove binaries, logs, runtime artifacts, etc... but keep the jars
function clean() {
    rm -rf client/ddlwindowing/*.class debugoutput \
           voltdbroot log catalog-report.html statement-plans
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    clean
    rm -rf ddlwindowing-client.jar
}

# compile the source code for the client into a jarfile
function jars() {
    # compile java source
    javac -target 1.7 -source 1.7 -classpath $CLIENTCLASSPATH client/ddlwindowing/*.java
    # build procedure and client jars
    jar cf ddlwindowing-client.jar -C client ddlwindowing
    # remove compiled .class files
    rm -rf client/ddlwindowing/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e voter-client.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    # run the server
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command line: "
    echo
    echo "voltdb create -l $LICENSE -H $HOST"
    echo
    voltdb create -l $LICENSE -H $HOST
}

# load schema
function init() {
    jars-ifneeded
    sqlcmd < ddl.sql
}

# Use this target for argument help
function client-help() {
    jars-ifneeded
    java -classpath $CLIENTCLASSPATH ddlwindowing.WindowingApp --help
}

## USAGE FOR CLIENT TARGET ##
# usage: ddlwindowing.WindowingApp
#     --displayinterval <arg>   Interval for performance feedback, in
#                               seconds.
#     --duration <arg>          Duration, in seconds.
#                               history target.
#     --password <arg>          Password for connection.
#     --ratelimit <arg>         Maximum TPS rate for inserts.
#     --servers <arg>           Comma separated list of the form
#                               server[:port] to connect to.
#     --user <arg>              User name for connection.

function client() {
    jars-ifneeded
    # Note that in the command below, maxrows and historyseconds can't both be non-zero.
    java -classpath $CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        ddlwindowing.WindowingApp \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost:21212 \
        --ratelimit=20000
}

function help() {
    echo "Usage: ./run.sh {clean|cleanall|jars|server|init|client|client-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
