#!/usr/bin/env bash

# set CLASSPATH
if [ -d "$(dirname $(which voltdb))" ]; then
    # tar.gz install
    CP="$(ls -1 $(dirname $(dirname "$(which voltdb)"))/voltdb/voltdbclient-*.jar)"
else
    echo "VoltDB client library not found.  If you installed with the tar.gz file, you need to add the bin directory to your PATH"
    exit
fi

SRC=`find client/src -name "*.java"`

if [ ! -z "$SRC" ]; then
    mkdir -p client/obj
    javac -classpath $CP -d client/obj $SRC
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi

    jar cf client/client.jar -C client/obj .
    rm -rf client/obj

    java -classpath "client/client.jar:$CP" org.voltdb.example.Benchmark $*

fi
