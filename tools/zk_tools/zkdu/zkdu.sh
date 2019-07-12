#!/usr/bin/env bash

# set CLASSPATH
if [ -d "$(dirname $(which voltdb))" ]; then
    CP="$(ls -1 $(dirname $(dirname "$(which voltdb)"))/voltdb/voltdb-*.jar)"
else
    echo "VoltDB library not found.  You need to add the voltdb bin directory to your PATH"
    exit
fi

SRC=`find src -name "*.java"`

if [ ! -z "$SRC" ]; then
    mkdir -p obj
    javac -classpath $CP -d obj $SRC
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi

    jar cf zkdu.jar -C obj .
    rm -rf obj

    java -classpath "zkdu.jar:$CP" org.voltdb.tools.ZKDU $*

fi
