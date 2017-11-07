#!/usr/bin/env bash

APPNAME="udfbenchmark"
DDL=ddl.sql

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(dirname $(dirname $(dirname $(pwd))))/bin"
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
VOLTDB="$VOLTDB_BIN/voltdb"
SQLCMD="$VOLTDB_BIN/sqlcmd"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj $APPNAME.jar voltdbroot log udfstats-*
}

# compile the source code for procedures and the client
function jars() {
    mkdir -p obj
    javac -classpath $APPCLASSPATH -d obj \
        $APPNAME/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    pushd obj > /dev/null
    jar cvf ../$APPNAME.jar $APPNAME/UDFLib*.class
    popd > /dev/null
}

function jarsifneeded() {
    if [ ! -e $APPNAME.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    # truncate the voltdb log
    [[ -d log && -w log ]] && > voltdbroot/log/volt.log
    # run the server
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command line: "
    echo
    echo "${VOLTDB} init --force -C deployment.xml"
    echo "${VOLTDB} start -H ${HOST} -l ${LICENSE}"
    echo
    ${VOLTDB} init --force -C deployment.xml
    ${VOLTDB} start -H ${HOST} -l ${LICENSE}
}

# run the client that drives the example
function client() {
    $APPNAME
}

function udfbenchmark() {
    jarsifneeded;
    $SQLCMD --stop-on-error=false < ddl.sql
    rm -f udfstats-*
    java -classpath obj:$APPCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        $APPNAME.UDFBenchmark \
        --displayinterval=5 \
        --servers=localhost \
        --datasize=10000 \
        --statsfile=udfstats-`date '+%Y-%m-%d'`
}

function help() {
    echo "Usage: ./run.sh {clean|jars|server|client|$APPNAME}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
