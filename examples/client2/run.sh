#!/usr/bin/env bash

# Compile and run one of the simple Client2 examples

if [ -z "$1" ]; then
    echo "Usage: $0 NAME"
    echo "  where NAME is the example file to run"
    exit
fi
NAME=${1%.java}
shift

if [ -d "$(dirname $(which voltdb))" ]; then
    CP="$(ls -1 $(dirname $(dirname "$(which voltdb)"))/voltdb/voltdbclient-*.jar)"
else
    echo "VoltDB client library not found. If you installed with the tar.gz file, you need to add the bin directory to your PATH"
    exit
fi

mkdir -p obj
javac -classpath $CP -d obj $NAME.java
if [ $? != 0 ]; then exit; fi

jar cf example.jar -C obj .
rm -rf obj

java -classpath "example.jar:$CP" org.voltdb.example.$NAME $*
rm example.jar
