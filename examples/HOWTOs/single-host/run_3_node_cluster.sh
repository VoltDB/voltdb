#! /bin/bash
set -o errexit #exit on any single command fail

# verify VoltDB is installed
voltdb --version

# setup working directory
export WORKING_DIR=$HOME/voltdb_instances
mkdir -p $WORKING_DIR
cd $WORKING_DIR


# verify license file is present
LICENSE="$WORKING_DIR/license.xml"
if [ ! -f $LICENSE ]; then
    echo "File $LICENSE not found!"
    echo "Copy your license file to $LICENSE before proceeding."
    exit 1
fi


# generate minimal deployment.xml file with 4 sitesperhost
echo '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' > deployment.xml
echo '<deployment>' >> deployment.xml
echo '    <cluster sitesperhost="4"/>' >> deployment.xml
echo '</deployment>' >> deployment.xml


# create working subfolders for each of the nodes
mkdir -p node1
mkdir -p node2
mkdir -p node3

# initialize each node
echo "initializing voltdb nodes"
voltdb init -C deployment.xml -D $WORKING_DIR/node1 -l license.xml
voltdb init -C deployment.xml -D $WORKING_DIR/node2 -l license.xml
voltdb init -C deployment.xml -D $WORKING_DIR/node3 -l license.xml

echo "starting voltdb nodes"
# start the first node using default ports
nohup voltdb start \
      -H localhost:3021,localhost:3022,localhost:3023 \
      -D $WORKING_DIR/node1 \
      --internal=3021 \
      --http=8080 \
      --admin=21211 \
      --client=21212 \
      --zookeeper=7181 \
      --replication=5555 \
      >> $WORKING_DIR/node1/console.log 2>&1 &

# start the next nodes using incremented ports
nohup voltdb start  \
      -H localhost:3021,localhost:3022,localhost:3023 \
      -D $WORKING_DIR/node2 \
      --internal=3022 \
      --http=8081 \
      --admin=21221 \
      --client=21222 \
      --zookeeper=7182 \
      --replication=5565 \
      >> $WORKING_DIR/node2/console.log 2>&1 &

nohup voltdb start \
      -H localhost:3021,localhost:3022,localhost:3023 \
      -D $WORKING_DIR/node3 \
      --internal=3023 \
      --http=8082 \
      --admin=21231 \
      --client=21232 \
      --zookeeper=7183 \
      --replication=5575 \
      >> $WORKING_DIR/node3/console.log 2>&1 &

# verify that the cluster started
until echo "exec @SystemInformation OVERVIEW;" | sqlcmd > /dev/null 2>&1
do
    sleep 2
    echo "  waiting for cluster to start..."
    if [[ $SECONDS -gt 60 ]]
    then
        echo "Exiting.  VoltDB cluster did not startup within 60 seconds. Check the logs to see what went wrong." 1>&2; exit 1;
    fi
done
echo "VoltDB cluster started!"
