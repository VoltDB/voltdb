#!/usr/bin/env bash

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
CLASSESJARS="uac_base.jar"

# remove binaries, logs, runtime artifacts, etc... but keep the jars
function clean() {
    rm -rf voltdbroot log procedures/uac/* client/uac/*.class \
     *.log jars/* ddlbase.sql uac-client.jar ben.config
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    # compile java source
    javac -classpath $APPCLASSPATH procedures/uac/*.java

    # build procedure and client jars
    if [ $# -gt 0 ]; then 
        CLASSESJARS="${1}"
	fi
	if [ ! -d "jars" ]; then
		mkdir jars
	fi
	
    jar cf "jars/${CLASSESJARS}" -C procedures uac

    # remove compiled .class files
    rm -rf procedures/uac/*.class

    jar_client
}

function jar_client() {
    javac -classpath $CLIENTCLASSPATH client/uac/*.java
    jar cf uac-client.jar -C client uac
    rm -rf client/uac/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e jars/${CLASSESJARS} ] || [ ! -e uac-client.jar ]; then
        jars;
    fi
}

# Init to directory voltdbroot
function voltinit-ifneeded() {
    voltdb init --force
}

# run the voltdb server locally
function server() {
    jars-ifneeded
    voltinit-ifneeded
    voltdb start -H $STARTUPLEADERHOST
}

function prepare() {
	clean
	echo "Expect 5 parameter: benchmark_name invocation_times table_count procedure_count batch_size"
	touch ben.config
	if [ $# -eq 5 ]; then
			# write a config file 
		echo "BENCHMARK_NAME=${1}"$'\n' >> ben.config
		echo "INVOCATIONS=${2}"$'\n' >> ben.config
		echo "TABLE_COUNT=${3}"$'\n' >> ben.config
		echo "PROCEDURE_COUNT=${4}"$'\n' >> ben.config
		echo "BATCH_SIZE=${5}"$'\n' >> ben.config
	else
		echo "Received ${#} parameters. Prepare for the default config"
		# write a config file 
		echo "BENCHMARK_NAME=ADD" >> ben.config
		echo "INVOCATIONS=5" >> ben.config
		echo "TABLE_COUNT=500" >> ben.config
		echo "PROCEDURE_COUNT=1000" >> ben.config
		echo "BATCH_SIZE=1" >> ben.config
	fi
	
	source ben.config
	python uacbench.py -n ${BENCHMARK_NAME} -i ${INVOCATIONS} -t ${TABLE_COUNT} -p ${PROCEDURE_COUNT} -b ${BATCH_SIZE}
}

# load schema and procedures
function init() {
    jars-ifneeded
    sqlcmd < ddlbase.sql
}

# run the client that drives the example
function client() {
    benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function benchmark-help() {
    jars-ifneeded
    java -classpath uac-client.jar:$CLIENTCLASSPATH uac.UpdateClassesBenchmark --help
}

# latencyreport: default is OFF
# ratelimit: must be a reasonable value if lantencyreport is ON
# Disable the comments to get latency report
function benchmark() {
	if [ ! -e ben.config ]; then
		prepare
    fi
    source ben.config

    jars-ifneeded
    java -classpath uac-client.jar:$CLIENTCLASSPATH uac.UpdateClassesBenchmark \
        --servers=$SERVERS \
        --dir=${PWD} \
        --name=${BENCHMARK_NAME} \
        --invocations=${INVOCATIONS} \
        --tablecount=${TABLE_COUNT} \
        --procedurecount=${PROCEDURE_COUNT} \
        --batchsize=${BATCH_SIZE}
}

function help() {
    echo "Usage: ./run.sh {clean|jars|prepare|server|init|client|benchmark|benchmark-help}"
}

# Run the targets pass on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then server; exit; fi
cmdargs=()
if [ $# -gt 1 ]; then
    for ((i=2; i<=$#; i++)); do
    	# ${!i} reference the value in the input args
		cmdargs+=" ${!i}"
	done
fi

echo "${0}: Performing ${1}${cmdargs}..."
${1} ${cmdargs}
