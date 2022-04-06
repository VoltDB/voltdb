#!/bin/sh -x

PUSH=1
[ "$1" = "--no-push" ] && PUSH=0

[ ! -e dusbench.jar ] && echo "ERROR: client jar missing.  build_docker.sh via 'run.sh jar'" 2>&1 && exit 1

rm -rf voltdb
mkdir -p voltdb/lib
mkdir -p voltdb/bin
mkdir -p voltdb/voltdb

VOLTDB_HOME=../../../


# copy over sqlcmd and voltdb libraries so sqlcmd can be run in container
cp $VOLTDB_HOME/lib/*.jar voltdb/lib
cp $VOLTDB_HOME/bin/sqlcmd voltdb/bin/
cp $VOLTDB_HOME/voltdb/* voltdb/voltdb/
cp $VOLTDB_HOME/voltdb/log4j.xml  voltdb/

./run.sh jars

docker build -t voltdb/delete-update-snapshot-benchmark:latest .

[ $PUSH -eq 1 ] && docker push voltdb/delete-update-snapshot-benchmark:latest
