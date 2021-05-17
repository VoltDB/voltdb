#!/usr/bin/env bash

## --- The following environtment variable setting is borrowed and
## --- slightly modified from the Voter example.

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(dirname $(dirname $(dirname $(pwd))))/bin"
    echo "The VoltDB scripts are not in your PATH."
    echo "For ease of use, add the VoltDB bin directory: "
    echo
    echo $VOLTDB_BIN
    echo
    echo "to your PATH."
    echo
fi
# move voltdb commands into path for this script
PATH=$VOLTDB_BIN:$PATH

# installation layout has all libraries in $VOLTDB_ROOT/lib/voltdb
if [ -d "$VOLTDB_BIN/../lib/voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib/voltdb"
    VOLTDB_VOLTDB="$VOLTDB_LIB"
# distribution layout has libraries in separate lib and voltdb directories
else
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib"
    VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"
fi

APPCLASSPATH=$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
CLIENTCLASSPATH=client.jar:$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdbclient-*.jar; \
} 2> /dev/null | paste -sd ':' - )
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# wait for backgrounded server to start up
function wait_for_startup() {
    until echo "exec @SystemInformation, OVERVIEW;" | sqlcmd > /dev/null 2>&1
    do
        sleep 2
        echo " ... Waiting for VoltDB to start"
        if [[ $SECONDS -gt 60 ]]
        then
            echo "Exiting.  VoltDB did not startup within 60 seconds" 1>&2; exit 1;
        fi
    done
}

## ---


if [ $# -lt 2 ]; then
    echo "Specify total rows and number of distinct values"
    exit 1
fi

pid=`jps | grep VoltDB | sed s/VoltDB//`
if [ "$pid" != "" ]; then
    echo Killing $pid...
    kill $pid
    sleep 1
fi

rm -f *.jar
rm -f procedures/approxcountdistinct/*.class
rm -f client/approxcountdistinct/*.class

javac -classpath $APPCLASSPATH procedures/approxcountdistinct/*.java
if (($? != 0)); then
    echo "Error compiling procedures"
    exit 1
fi

jar cf procs.jar -C procedures/ approxcountdistinct
if (($? != 0)); then
    echo "Error creating stored procedure jar file"
    exit 1
fi


javac -classpath $CLIENTCLASSPATH client/approxcountdistinct/*.java
if (($? != 0)); then
    echo "Error compiling client"
    exit 1
fi

jar cf client.jar -C client/ approxcountdistinct
if (($? != 0)); then
    echo "Error creating client jar file"
    exit 1
fi

voltdb init
voltdb start &
wait_for_startup

sqlcmd < ddl.sql

export CLASSPATH=${CLASSPATH}:${PWD}/client.jar

java -classpath $CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
     approxcountdistinct.Benchmark --numTotalVals $1 --numUniqueVals $2
