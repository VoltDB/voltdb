#!/usr/bin/env bash

VOLTJAR=`ls ../../voltdb/voltdb-2.*.jar`
CLASSPATH="$VOLTJAR:../../lib"
VOLTDB="../../bin/voltdb"
VOLTCOMPILER="../../bin/voltcompiler"
LICENSE="../../voltdb/license.xml"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput voltcache.jar voltdbroot plannerlog.txt voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    echo $CLASSPATH
    javac -classpath $CLASSPATH -d obj \
        src/com/*.java \
        src/com/api/*.java \
        src/com/procedures/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function compile() {
    srccompile
    $VOLTCOMPILER obj project.xml voltcache.jar
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f voltcache.jar ]; then compile; fi
    # run the server
    $VOLTDB create catalog voltcache.jar deployment deployment.xml \
        license $LICENSE leader localhost
}

# run the client that drives the example
function client() {
    benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj com.Benchmark --help
}

function benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj com.Benchmark \
        --threads=40 \
        --display-interval=5 \
        --duration=120 \
        --servers=localhost \
        --port=21212 \
        --pool-size=100000 \
        --preload=true \
        --get-put-ratio=0.90 \
        --key-size=32 \
        --min-value-size=1024 \
        --max-value-size=1024 \
        --use-compression=false
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then echo "Too many arguments to script"; exit; fi
if [ $# = 1 ]; then $1; else server; fi
