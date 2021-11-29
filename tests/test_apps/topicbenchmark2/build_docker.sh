#!/bin/sh -x
#
# Build and push docker image for TopicBenchmark2
# Pushes by default to voltdb/workloads-topicbenchmark2:latest
# Optional argument mays specify another image, but must ne complete i.e.
# repo/image-name[:tag]


mkdir -p voltdb/lib
mkdir -p voltdb/bin
mkdir -p voltdb/voltdb

VOLTDB_HOME=../../../
IMAGE=voltdb/workloads-topicbenchmark2:latest
if [[ -n $1 ]]; then
  IMAGE=$1
fi

echo "Building and pushing $IMAGE"

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

docker build -t $IMAGE .
docker push $IMAGE

./run.sh clean
rm -rf voltdb
