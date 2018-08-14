#!/usr/bin/env bash

# set CLASSPATH
if [ -d "$(dirname $(which voltdb))" ]; then
    # tar.gz install
    CP="$(ls -1 $(dirname $(dirname "$(which voltdb)"))/voltdb/voltdbclient-*.jar)"
else
    echo "VoltDB client library not found.  If you installed with the tar.gz file, you need to add the bin directory to your PATH"
    exit
fi

SRC=`find . -name "*.java"`

if [ ! -z "$SRC" ]; then
    mkdir -p obj
    javac -classpath $CP -d obj $SRC
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi

    jar cf client.jar -C obj .
    rm -rf obj

    java -classpath "client.jar:$CP" ExampleApp $*

fi
