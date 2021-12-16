#!/usr/bin/env bash

APPNAME="voter"

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(pwd)/../../../bin"
fi
# installation layout has all libraries in $VOLTDB_ROOT/lib/voltdb
if [ -d "$VOLTDB_BIN/../lib/voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib/voltdb"
    VOLTDB_VOLTDB="$VOLTDB_LIB"
# distribution layout has libraries in separate lib and voltdb directories
elif [ -d "$VOLTDB_BIN/../voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib"
    VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"
else
    VOLTDB_LIB="`pwd`/../../../lib"
    VOLTDB_VOLTDB="`pwd`/../../../voltdb"
fi

CLASSPATH=$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
VOLTDB="$VOLTDB_BIN/voltdb"
VOLTCOMPILER="$VOLTDB_BIN/voltcompiler"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput ${APPNAME}.jar voltdbroot log voltdb_crash*txt
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    mkdir -p obj
    # compile java source
    javac -classpath $CLASSPATH -d obj \
        src/${APPNAME}/*.java \
        src/${APPNAME}/procedures/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit 1; fi
    # build the jar file
    jar cf ${APPNAME}.jar -C obj ${APPNAME}
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
    echo "To perform this action manually, use the command lines: "
    echo
    echo "${VOLTDB} init -C deployment$1.xml -j ${APPNAME}.jar -s ddl.sql --force"
    echo "${VOLTDB} start -l ${LICENSE} -H ${HOST}"
    echo
    ${VOLTDB} init -C deployment$1.xml -j ${APPNAME}.jar -s ddl.sql --force
    ${VOLTDB} start -l ${LICENSE} -H ${HOST}
}

# run the voltdb server locally
function secure-server() {
    server -secure
}

function masked-server() {
    ${VOLTDB} mask deployment-secure.xml deployment-masked.xml
    server -masked
}

# run the client that drives the example
function client() {
    async-benchmark
}

# generic benchmark function called by all the FOO-benchmark functions below,
# with various class names and extra arguments
function benchmark() {
    jars-ifneeded
    CLASSNAME=${1}Benchmark
    shift
    java -classpath ${CLASSPATH}:${APPNAME}.jar -Dlog4j.configuration=file://$LOG4J \
        voter.${CLASSNAME} \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --contestants=6 \
        --maxvotes=2 \
        $@
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    jars-ifneeded
    java -classpath ${CLASSPATH}:${APPNAME}.jar voter.AsyncBenchmark --help
}

function async-benchmark() {
    benchmark Async \
        --ratelimit=100000
}

function secure-benchmark() {
    async-benchmark --username=myuser --password=voltdbuser
}

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
function sync-benchmark-help() {
    jars-ifneeded
    java -classpath ${CLASSPATH}:${APPNAME}.jar voter.SyncBenchmark --help
}

function sync-benchmark() {
    benchmark Sync --threads=40
}

# JDBC benchmark sample
# Use this target for argument help
function jdbc-benchmark-help() {
    jars-ifneeded
    java -classpath ${CLASSPATH}:${APPNAME}.jar voter.JDBCBenchmark --help
}

function jdbc-benchmark() {
    benchmark JDBC --threads=40
}

function help() {
    echo "
  Usage: ./run.sh OPTION

  Options:
      clean | jars |
      server | secure-server | masked-server | client N |
      async-benchmark | sync-benchmark | jdbc-benchmark |
      async-benchmark-help | sync-benchmark-help | jdbc-benchmark-help
"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
