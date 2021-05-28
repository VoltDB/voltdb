#!/usr/bin/env bash

APPNAME="voltkv"

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
# Jars needed to compile JDBC Benchmark. Apprunner uses a nfs shared path.
CONNECTIONPOOLLIB=${CONNECTIONPOOLLIB:-"/home/opt/connection-pool"}
CONNECIONPOOLCLASSPATH=$({\
    \ls -1 "$CONNECTIONPOOLLIB"/tomcat-*.jar; \
    \ls -1 "$CONNECTIONPOOLLIB"/mchange-commons-java-*.jar; \
    \ls -1 "$CONNECTIONPOOLLIB"/c3p0-*.jar; \
    \ls -1 "$CONNECTIONPOOLLIB"/bonecp-*.jar; \
    \ls -1 "$CONNECTIONPOOLLIB"/HikariCP-*.jar; \
    \ls -1 "$CONNECTIONPOOLLIB"/guava-*.jar; \
    \ls -1 "$CONNECTIONPOOLLIB"/slf4j-*.jar; \
} 2> /dev/null | paste -sd ':' - )
CLASSPATH="$CONNECIONPOOLCLASSPATH:$CLASSPATH:`pwd`"
VOLTDB="$VOLTDB_BIN/voltdb"
VOLTCOMPILER="$VOLTDB_BIN/voltcompiler"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf build obj debugoutput $APPNAME*.jar voltdbroot log
}

# load schema and procedures
function init-security() {
    jars-ifneeded
    sqlcmd < ddl-security.sql
}

# load schema and procedures
function init-export() {
    jars-ifneeded
    sqlcmd < ddl_withexport.sql
}

# load schema and procedures
function init() {
    jars-ifneeded
    sqlcmd < ddl.sql
}

# migration away from catalog
function jars() {
    ant -f build.xml all
}


# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e voltkv.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    jars-ifneeded
    $VOLTDB create -d deployment.xml -l $LICENSE -H `hostname`
    echo Run "init" step when server startup is complete
}

function exportserver() {
    jars-ifneeded
    # run the server
    $VOLTDB create -d deployment_export.xml -l $LICENSE -H $HOST
    echo Run "init-export" step when server startup is complete
}

# run the client that drives the example
function client() {
    async-benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    jars-ifneeded
    java -classpath $APPNAME.jar:$CLASSPATH voltkvqa.AsyncBenchmark --help
}

function async-benchmark() {
    jars-ifneeded
    java -classpath $APPNAME.jar:$CLASSPATH -Dlog4j.configuration=file://$LOG4J \
        voltkvqa.AsyncBenchmark \
        --displayinterval=5 \
        --duration=60 \
        --servers=localhost \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.9 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --entropy=127 \
        --usecompression=false \
        --ratelimit=100000 \
        --multisingleratio=0.1 \
        --recover=false
}

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
function sync-benchmark-help() {
    jars-ifneeded
    java -classpath $APPNAME.jar:$CLASSPATH voltkvqa.SyncBenchmark --help
}

function sync-benchmark() {
    jars-ifneeded
    java -classpath $APPNAME.jar:$CLASSPATH -Dlog4j.configuration=file://$LOG4J \
        voltkvqa.SyncBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --usecompression=false \
        --threads=40
}

function http-benchmark() {
    jars-ifneeded
    java -classpath $APPNAME.jar:$CLASSPATH -Dlog4j.configuration=file://$LOG4J \
        voltkvqa.HTTPBenchmark \
        --displayinterval=5 \
        --duration=60 \
        --servers=localhost \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --usecompression=false \
        --warmup=15 \
        --threads=35
}

# Use this target for argument help
function jdbc-benchmark-help() {
    jars-ifneeded
    java -classpath obj:$CLASSPATH voltkvqa.JDBCBenchmark --help
}

function jdbc-benchmark() {
    jars-ifneeded
    java -classpath $APPNAME.jar:$CLASSPATH -Dlog4j.configuration=file://$LOG4J \
        voltkvqa.JDBCBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --usecompression=false \
        --threads=40
}

function jdbc-benchmark-c3p0() {
    jars-ifneeded
    java -classpath $APPNAME.jar:$CLASSPATH -Dlog4j.configuration=file://$LOG4J \
        voltkvqa.JDBCBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --usecompression=false \
        --externalConnectionPool=c3p0 \
        --threads=40
}

function jdbc-benchmark-tomcat() {
    jars-ifneeded
    java -classpath $APPNAME.jar:$CLASSPATH -Dlog4j.configuration=file://$LOG4J \
        voltkvqa.JDBCBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --usecompression=false \
        --externalConnectionPool=tomcat \
        --threads=40
}

function jdbc-benchmark-bonecp() {
    jars-ifneeded
    java -classpath $APPNAME.jar:$CLASSPATH -Dlog4j.configuration=file://$LOG4J \
        voltkvqa.JDBCBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --usecompression=false \
        --externalConnectionPool=bonecp \
        --threads=40
}

function jdbc-benchmark-hikari() {
    jars-ifneeded
    java -classpath $APPNAME.jar:$CLASSPATH -Dlog4j.configuration=file://$LOG4J \
        voltkvqa.JDBCBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --usecompression=false \
        --externalConnectionPool=hikari \
        --threads=40
}

function help() {
    echo "Usage: ./run.sh {clean|init|server|async-benchmark|aysnc-benchmark-help|...}"
    echo "       {...|sync-benchmark|sync-benchmark-help|jdbc-benchmark-*|jdbc-benchmark-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
