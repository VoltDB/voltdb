#!/usr/bin/env bash

APPNAME="voltcache"
CLASSPATH="`ls -x ../../voltdb/voltdb-*.jar | tr '[:space:]' ':'``ls -x ../../lib/*.jar | tr '[:space:]' ':'`"
VOLTDB="../../bin/voltdb"
VOLTCOMPILER="../../bin/voltcompiler"
LOG4J="`pwd`/../../voltdb/log4j.xml"
LICENSE="../../voltdb/license.xml"
LEADER="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar $APPNAME.api.jar voltdbroot voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -target 1.6 -classpath $CLASSPATH -d obj \
        src/voltcache/*.java \
        src/voltcache/api/*.java \
        src/voltcache/procedures/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi

    jar -cf $APPNAME.api.jar -C obj/ voltcache/api/
}

# build an application catalog
function catalog() {
    srccompile
    $VOLTCOMPILER obj project.xml $APPNAME.jar
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment deployment.xml \
        license $LICENSE leader $LEADER
}

# run the client that drives the example
function client() {
    benchmark
}

# Benchmark sample
# Use this target for argument help
function benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj voltcache.Benchmark --help
}

function benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        voltcache.Benchmark \
        --threads=40 \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --poolsize=100000 \
        --preload=true \
        --getputratio=0.90 \
        --keysize=32 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --usecompression=false
}

# Help on the Memcached Interface Server
function memcached-interface-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj voltcache.api.MemcachedInterfaceServer --help
}

# Provides a sample protocol transalation between VoltCache and Memcached, allowing
# client applications using a Memcached client to run on VoltCache without any code
# change (Text Protocol only)
function memcached-interface() {
    srccompile
    java -classpath obj:$CLASSPATH:obj voltcache.api.MemcachedInterfaceServer \
        --vservers=localhost \
        --mport=11211
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|server|benchmark|benchmark-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
