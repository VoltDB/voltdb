#!/bin/sh -x

mkdir -p voltdb/lib
mkdir -p voltdb/bin
mkdir -p voltdb/voltdb

VOLTDB_HOME=../../../


# if the docker context doesn't have the voltdb libraries and
# other files needed to allow sqlcmd to run, copy them in
# so they're available to the docker build command.

if [ ! -e voltdb/lib/felix-framework-5.6.12.jar ]; then
    cp $VOLTDB_HOME/lib/*.jar voltdb/lib
fi

if [ ! -e voltdb/bin/sqlcmd ]; then
    cp $VOLTDB_HOME/bin/sqlcmd voltdb/bin/
fi
cp $VOLTDB_HOME/voltdb/* voltdb/voltdb/
cp $VOLTDB_HOME/voltdb/log4j.xml  voltdb/

./run.sh jars

docker build -t voltdb/kvbenchmark:latest .
docker push voltdb/kvbenchmark:latest
