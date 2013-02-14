#!/usr/bin/env bash

APPNAME="voltcache"

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(pwd)/../../bin"
fi
# installation layout has all libraries in $VOLTDB_ROOT/lib/voltdb
if [ -d "$VOLTDB_BIN/../lib/voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib/voltdb"
    VOLTDB_VOLTDB="$VOLTDB_LIB"
# distribution layout has libraries in separate lib and voltdb directories
else
    VOLTDB_LIB="`pwd`/../../lib"
    VOLTDB_VOLTDB="`pwd`/../../voltdb"
fi

APPCLASSPATH=$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar $APPNAME.api.jar voltdbroot voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -target 1.6 -source 1.6 -classpath $APPCLASSPATH -d obj \
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
    $VOLTDB compile --classpath obj -o $APPNAME.jar ddl.sql
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment deployment.xml \
        license $LICENSE host $HOST
}

# run the client that drives the example
function client() {
    benchmark
}

# Benchmark sample
# Use this target for argument help
function benchmark-help() {
    srccompile
    java -classpath obj:$APPCLASSPATH:obj voltcache.Benchmark --help
}

function benchmark() {
    srccompile
    java -classpath obj:$APPCLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
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
    java -classpath obj:$APPCLASSPATH:obj voltcache.api.MemcachedInterfaceServer --help
}

# Provides a sample protocol transalation between VoltCache and Memcached, allowing
# client applications using a Memcached client to run on VoltCache without any code
# change (Text Protocol only)
function memcached-interface() {
    srccompile
    java -classpath obj:$APPCLASSPATH:obj voltcache.api.MemcachedInterfaceServer \
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
