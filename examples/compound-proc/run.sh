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

echo "+++" $VOLTDB_BIN "+++"
# call script to set up paths, including
# java classpaths and binary paths
source $VOLTDB_BIN/voltenv

# remove binaries, logs, runtime artifacts, etc...
function clean {
    rm -rf voltdbroot voltdb_crash* log
    rm -f order*.jar src/*.class
}

# compile the source code for procedures and the client into jarfiles
function jars {
    # compile java source
    javac -classpath $APPCLASSPATH src/OrderProc.java
    javac -classpath $CLIENTCLASSPATH src/OrderClient.java
    # build procedure and client jars
    jar cf orderproc.jar -C src OrderProc.class
    jar cf orderclient.jar -C src OrderClient.class
    # remove compiled .class files
    rm -f src/*.class
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded {
    if [ ! -e orderproc.jar ] || [ ! -e orderclient.jar ]; then
        jars;
    fi
}

# run the voltdb server locally
function server {
    jars-ifneeded
    voltdb init -f -j orderproc.jar -s ddl.sql
    voltdb start
}

# init the customer/parts tables
function init {
    sqlcmd --servers=$SERVERS < populate.sql
    echo " "
}

version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
add_open=
if [[ $version == 11.0* ]] || [[ $version == 17.0* ]] ; then
        add_open="--add-opens java.base/sun.nio.ch=ALL-UNNAMED"
fi

# run the client that drives the example
function client {
    jars-ifneeded
    java $add_open \
        -classpath orderclient.jar:$CLIENTCLASSPATH OrderClient
}

function help {
    echo "
Usage:  ./run.sh target...

Targets:
        help | jars | init | clean
        server | client

tl;dr:
        ./run.sh server         in one terminal
        ./run.sh init client    in another terminal
"
}

# Run the targets passed on the command line

if [ $# -eq 0 ];
then
    help
    exit 0
fi

for arg in "$@"
do
    echo "${0}: Performing $arg..."
    $arg
done
