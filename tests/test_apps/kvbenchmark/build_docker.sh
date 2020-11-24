#!/bin/sh -x

mkdir -p voltdb/lib
mkdir -p voltdb/bin
mkdir -p voltdb/voltdb
VOLTDB_HOME=../../../
if [ ! -e voltdb/lib/felix-framework-5.6.12.jar ]; then
    cp $VOLTDB_HOME/lib/*.jar voltdb/lib
fi
if [ ! -e voltdb/bin/sqlcmd ]; then
    cp $VOLTDB_HOME/bin/sqlcmd voltdb/bin/
fi
cp $VOLTDB_HOME/voltdb/* voltdb/voltdb/
#cp $VOLTDB_HOME/voltdb/log4j.xml  voltdb/

#cp $VOLTDB_HOME/voltdb/license.xml voltdb/
./run.sh jars

docker build -t voltdb/kvbenchmark .

#docker run -it -v $(pwd):/build --network=voltdb_network -p 21212:21212 -p 21211:21211 -p 8081:8080 -p 3021:3021 -p 5555:5555 -p 7181:7181 -w /build --name="voltdb" voltdb/voltdb-enterprise:latest bash -c "cd /opt/voltdb/bin && ./voltdb init && ./voltdb start"
# verify it is reachable locally:
#/workspace/VoltDB/voltdb/bin (master)$>./sqlcmd
#SQL Command :: localhost:21212
#1>
# verify we can reach it from another container
# docker run --network=voltdb_network --name=kvbenchmark jwiebe/kvbenchmark

# try sqlcmd on inside that container
#docker exec kvbenchmark bash -c "/kvbenchmark/voltdb/bin/sqlcmd --servers=voltdb --query='select * from store'"

#select * from store;
#SQL error while compiling query: Error in "select * from store" - object not found: STORE

# ~/workspace/VoltDB/voltdb/tests/test_apps/kvbenchmark (master)$>docker exec kvbenchmark /usr/bin/java -cp /kvbenchmark/kvbenchmark.jar:/kvbenchmark/voltdb/voltdb/voltdb-10.0.beta5.jar:/kvbencharmmark/voltdb/lib/*.jar kvbench.SyncBenchmark --servers=voltdb

docker tag voltdb/kvbenchmark:latest localhost:5000/kvbenchmark:latest

docker push localhost:5000/kvbenchmark:latest

docker push voltdb/kvbenchmark:latest
