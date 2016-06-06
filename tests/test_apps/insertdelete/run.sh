#!/usr/bin/env bash

# leader host for startup purposes only
# (once running, all nodes are the same -- no leaders)
STARTUPLEADERHOST="localhost"
# list of cluster nodes separated by commas in host:[port] format
SERVERS="localhost"

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

# remove binaries, logs, runtime artifacts, etc... but keep the jars
function clean() {
    rm -rf voltdbroot log procedures/*.class client/benchmark/*.class
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    clean
    rm -rf procs.jar client.jar
}


# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -classpath $APPCLASSPATH procedures/*.java
    javac -classpath $CLIENTCLASSPATH client/benchmark/*.java
    # build procedure and client jars
    jar cf procs.jar -C . procedures
    jar cf client.jar -C client benchmark
    # remove compiled .class files
    rm -rf procedures/*.class client/benchmark/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e procs.jar ] || [ ! -e client.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server() {
    # note: "create --force" will delete any existing data
    # use "recover" to start from an existing voltdbroot folder with data
    voltdb create --force -H $STARTUPLEADERHOST
}

# load schema and procedures
function init() {
    jars-ifneeded
    sqlcmd < ddl.sql
}

# run this target to see what command line options the client offers
function client-help() {
    jars-ifneeded
    java -classpath client.jar:$CLIENTCLASSPATH benchmark.Benchmark --help
}

# run the client that drives the example with some editable options
# Here we run the tests with both inline and outofline data.
function client() {
    local DONE
    local ARG
    jars-ifneeded
    java -classpath client.jar:$CLIENTCLASSPATH benchmark.Benchmark $INLINE $OUTLINE $NUMITERS $SERVERS
}

function seed() {
    jars-ifneeded
    java -classpath client.jar:$CLIENTCLASSPATH benchmark.SeedTables $SERVERS
}


function help() {
    echo "Usage: ./run.sh {clean|cleanall|jars|server|init|client|client-help}"
}

# Run the targets pass on the command line
# If no first arg, run server
if [ -n "$1" ] ; then
    CMD="$1"
    shift
fi
while [ -z "$DONE" ] ; do
    case "$1" in
    --#)
        shift;
        ECHO=echo
        ;;
    --inline)
        shift;
        # Both --inline and --outline means nothing.
        if [ -n "$OUTLINE" ] ; then
            unset OUTLINE
        else
            INLINE="--inline"
        fi
        ;;
    --outline)
        shift;
        # Both --inline and --outline means nothing.
        if [ -n "$INLINE" ] ; then
            unset INLINE
        else
            OUTLINE="--outline"
        fi
        ;;
    --numiters)
        shift;
        NUMITERS="--numiters $1"
        shift
        ;;
    "")
        DONE=YES
        ;;
    *)
	shift
	CMD="$1"
	shift
        ;;
    esac
done

if [ -z "$CMD" ]; then server; exit; fi
echo "Performing $CMD"
$CMD
