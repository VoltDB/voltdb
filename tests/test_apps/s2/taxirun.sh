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
    \ls -1 "$VOLTDB_LIB"/commons-cli-1.2.jar; \
    \ls -1 "$VOLTDB_LIB"/super-csv-2.1.0.jar; \
} 2> /dev/null | paste -sd ':' - )
CLIENTCLASSPATH=${CLIENTCLASSPATH}:s2-src
echo "CLIENTCLASSPATH:$CLIENTCLASSPATH"
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

## --------------------------------------------------------------------

echo
echo
echo

pid=`jps | grep VoltDB | sed s/VoltDB//`
if [ "$pid" != "" ]; then
    echo "****** Killing running VoltDB Instance $pid ******"
    kill $pid
    sleep 1
fi

rm -f *.jar
rm -f procedures/iwdemo/*.class
rm -f client/iwdemo/*.class
rm -f s2-src/com/google/common/geometry/*.class

echo
echo
echo

echo "****** Compiling S2 ******"
javac -target 1.7 -source 1.7 -classpath ${APPCLASSPATH} s2-src/com/google_voltpatches/common/geometry/*.java
if (($? != 0)); then
    echo "Error compiling S2"
    exit 1
fi

echo "****** Compiling Procedures ******"
javac -target 1.7 -source 1.7 -classpath ${APPCLASSPATH}:s2-src procedures/iwdemo/*.java
if (($? != 0)); then
    echo "Error compiling procedures"
    exit 1
fi

jar cf procs.jar -C procedures/ iwdemo
jar uf procs.jar -C s2-src com
if (($? != 0)); then
    echo "Error creating stored procedure jar file"
    exit 1
fi

echo "****** Compiling Client ******"
echo "CLASSPATH=$CLIENTCLASSPATH"
javac -target 1.7 -source 1.7 -classpath $CLIENTCLASSPATH client/iwdemo/*.java
if (($? != 0)); then
    echo "Error compiling client"
    exit 1
fi

jar cf client.jar -C client/ iwdemo
if (($? != 0)); then
    echo "Error creating client jar file"
    exit 1
fi

echo
echo
echo


voltdb create &
wait_for_startup

sqlcmd < taxi_ddl.sql
if (($? != 0)); then
    echo "Error loading DDL"
    exit 1
fi

echo
echo
echo

java -classpath $CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
     iwdemo.Benchmark
