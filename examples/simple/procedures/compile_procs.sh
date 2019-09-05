#!/usr/bin/env bash

# set CLASSPATH
if [ -d "/usr/lib/voltdb" ]; then
    # .deb or .rpm install
    CP="$(ls -1 /usr/lib/voltdb/voltdb-*.jar)"
elif [ -d "$(dirname $(which voltdb))" ]; then
    # tar.gz install
    CP="$(ls -1 $(dirname $(dirname "$(which voltdb)"))/voltdb/voltdb-*.jar)"
else
    echo "VoltDB library not found.  If you installed with the tar.gz file, you need to add the bin directory to your PATH"
    exit
fi

SRC=`find src -name "*.java"`

if [ ! -z "$SRC" ]; then
    mkdir -p obj
    javac -classpath $CP -d obj $SRC
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi

    jar cf procedures.jar -C obj .
    rm -rf obj
fi
