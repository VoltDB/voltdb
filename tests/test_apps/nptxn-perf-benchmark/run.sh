#!/usr/bin/env bash

# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

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

# remove binaries, logs, runtime artifacts, etc...
function clean() {
    rm -f client/np/*.class procs/np/*.class
    rm -f *.jar stats
}

#################################
# cleanup and generate the jars #
#################################
function jars() {
    clean
    makeJars
}

###############################################
# start the server and initialize the schemas #
###############################################
function start() {
    server
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
function server() {
    clean
    makeJars
    voltdb init --force
    voltdb start -H "$STARTUPLEADERHOST"
}

###################
# Load the schema #
###################
function init() {
    sqlcmd < schema.sql
}

##################################
# Start the benchmarking program #
##################################
function client() {
    init
    java -classpath $BENCHMARK_JAR_NAME:$CLIENTCLASSPATH np.NPBenchmark \
         --servers="$SERVERS" \
         --sprate=0.1 \
         --cardcount=500000 \
         --mprate=0.005 \
         --skew=0.0 \
         --duration=25 \
         --clientscount=8 \
         --displayinterval=1
}

###################
# Print help info #
###################
function help() {
    echo "Usage: run.sh [cmd]"
    echo "[cmd] option: "
    echo "             [NO_OPTION]: same as running \"run.sh server\""
    echo "             server: generate the need jars, initialize and start the voltdb instance"
    echo "             client: start running the benchmark"
    echo "             clean: clean the temporary files"
}

if [ $# -eq 0 ]; then server; exit; fi
cmdargs=()
if [ $# -gt 1 ]; then
    help
fi

echo "${0}: Performing ${1} ${cmdargs}..."
${1} ${cmdargs}

