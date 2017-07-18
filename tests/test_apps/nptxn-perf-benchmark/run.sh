#!/usr/bin/env bash

#
# Ported and modified from https://github.com/VoltDB/voltdb/pull/3822
#

# find voltdb binaries
if [ -e ../../bin/voltdb ]; then
    # assume this is the examples folder for a kit
    VOLTDB_BIN="$(dirname $(dirname $(pwd)))/bin"
elif [ -n "$(which voltdb 2> /dev/null)" ]; then
    # assume we're using voltdb from the path
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    echo "Unable to find VoltDB installation."
    echo "Please add VoltDB's bin directory to your path."
    exit -1
fi

# call script to set up paths, including
# java classpaths and binary paths
source $VOLTDB_BIN/voltenv

# leader host for startup purposes only
# (once running, all nodes are the same -- no leaders)
STARTUPLEADERHOST="localhost"
# list of cluster nodes separated by commas in host:[port] format
SERVERS="localhost"

PROCS_JAR_NAME="np-procs.jar"
BENCHMARK_JAR_NAME="np-benchmark.jar"

# remove binaries, logs, runtime artifacts, etc... but keep the jars
function cleanUp() {
    echo "Clean up..."
}

#################################
# cleanup and generate the jars #
#################################
function prepare() {
    cleanUp
    makeJars
}

###############################################
# start the server and initialize the schemas #
###############################################
function start() {
    server-init
}

# Compile the classes, generate the bundled jar file(s)
function makeJars() {
    javac -cp "$APPCLASSPATH" ./procs/np/*.java
    jar cf $PROCS_JAR_NAME -C procs np

    javac -cp "$CLIENTCLASSPATH" ./client/np/*.java
    jar cf $BENCHMARK_JAR_NAME -C client np

    rm -f client/np/*.class procs/np/*.class
}

# Start the server, and load the initial schema from *.sql
function server-init() {
    voltdb init --force
    voltdb start -H "$STARTUPLEADERHOST"
}

# Load the schema
function load() {
    sqlcmd < schema.sql
}

##################################
# Start the benchmarking program #
##################################
function client() {
    java -classpath $BENCHMARK_JAR_NAME:$CLIENTCLASSPATH np.NPBenchmark \
         --scale='0.5' \
         --cardcount='500000' \
         --mprate='0.02' \
         --skew='0.99'
}

if [ $# -eq 0 ]; then server-init; exit; fi
cmdargs=()
if [ $# -gt 1 ]; then
    for ((i=2; i<=$#; i++)); do
    	# ${!i} reference the value in the input args
		cmdargs+=" ${!i}"
	done
fi

echo "${0}: Performing ${1} ${cmdargs}..."
${1} ${cmdargs}

