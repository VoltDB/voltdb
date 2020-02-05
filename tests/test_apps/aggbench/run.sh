#!/usr/bin/env bash

# these are usually the same, but not in this case
APPNAME="aggbenchmark"
APPDIR="aggregationbenchmark"

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
VOLTADMIN="$VOLTDB_BIN/voltadmin"

LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput ${APPNAME}.jar voltdbroot statement-plans catalog-report.html log
}

# compile the source code for the client into jarfiles
function jars() {
    mkdir -p obj
    # compile java source
    javac -classpath $APPCLASSPATH -d obj \
        src/${APPDIR}/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit 1; fi
    # build the jar file
    jar cf ${APPNAME}.jar -C obj ${APPDIR}
    if [ $? != 0 ]; then exit 2; fi
    # remove compiled .class files
    rm -rf obj
}

# compile the jar file, if it doesn't exist
function jars-ifneeded() {
    if [ ! -e ${APPNAME}.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    jars-ifneeded
    # truncate the voltdb log
    [[ -d voltdbroot/log && -w voltdbroot/log ]] && > voltdbroot/log/volt.log
    # run the server
    echo "Starting the VoltDB server."
    echo "To perform this action manually, use the command line: "
    echo
    echo "${VOLTDB} init -C deployment.xml -j ${APPNAME}.jar -s ddl.sql --force"
    echo "${VOLTDB} start -l ${LICENSE} -H ${HOST}"
    echo
    ${VOLTDB} init -C deployment.xml -j ${APPNAME}.jar -s agg_ddl.sql --force
    ${VOLTDB} start -l ${LICENSE} -H ${HOST}
}

function restore() {
    ${VOLTADMIN} restore /tmp/aggbench/backup "TestSnapshot"
}

function sqlclient() {
#   for x in `seq 1 5`; do echo "Query $x" ; echo "exec Q${x}"| ${SQLCMD} | grep "rows" ; done
    for x in `seq 1 5`; do echo "Query $x" ; echo "exec Q${x}"| ${SQLCMD} | grep "rows" ; done
}

# run the client that drives the example
function client() {
    jars-ifneeded
    java -classpath ${APPCLASSPATH}:${APPNAME}.jar -Dlog4j.configuration=file://$LOG4J \
        aggregationbenchmark.AggregationBenchmark \
        --servers=localhost \
        --restore=0 \
        --proc=1 \
        --invocations=6 \
        --statsfile="stats"
}

function help() {
    echo "Usage: ./run.sh {clean|jars[-ifneeded]|server|restore|sqlclient|client}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
