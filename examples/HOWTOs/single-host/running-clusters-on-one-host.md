Running VoltDB Clusters on a Single Host
========================================

This is not recommended for production use, but sometimes for development and testing purposes you want to run one or more VoltDB clusters on a single host. This may be because you need to test some clustering features or reproduce a troubleshooting scenario, and you don't want to take the time to deploy to the cloud or to configure Docker.

VoltDB runs natively on Linux or Mac, and there are only two things that restrict you from running multiple instances on the same host:

1. Each process uses network ports, but you can't use the same ports
2. Each process writes to certain files on disk, so each process needs its own directory to run in.

These obstacles are fairly easy to overcome in the following way:

1. Use optional command-line arguments to override the ports used by each instance, and give them unique port numbers.

2. Run each instance in its own directory.

3. Use nohup to redirect stdout and stderr, rather than the -B, --background parameter, which would output to the same directory.


For example, you might create folders and initialize VoltDB instances like this:

    WORKING_DIR=$HOME/voltdb_instances
    mkdir -p $WORKING_DIR
    cd $WORKING_DIR
    mkdir -p node1
    mkdir -p node2
    mkdir -p node3

Then, initialize each instance using a single deployment.xml file, along with the file containing the VoltDB license:

    voltdb init -C deployment.xml -D $WORKING_DIR/node1 -l license.xml
    voltdb init -C deployment.xml -D $WORKING_DIR/node2 -l license.xml
    voltdb init -C deployment.xml -D $WORKING_DIR/node3 -l license.xml

Now you can start each instance to form a cluster. It is convenient to use the default ports for the first instance, that way clients can connect without having to specify a port. In this case, for a small cluster, we don't need to specify the --count parameter, we can just list all the hosts in the -H (--hosts) parameter. But since all of the nodes will be running on localhost, they must be distinguished by specifying their internal port numbers. We also use the -D parameter to specify the node1 directory that we created for it to run in, and we redirect the nohup stdout and stderr to node1/console.log.

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

The remaining nodes can be started with a similar command, where we have changed the directory and the port numbers. Because the default admin and client ports are adjacent values 21211 and 21212, we can increment by 10. In some older versions, the replication port specified defined a range of 3 adjacent ports, so we can increment the replication port by 10 also.

    nohup voltdb start \
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

At this point, the 3 nodes have started and they will soon connect to each other and form a cluster. This may take a moment. We will know it has been successful when we see "Server completed initialization" in the logs, or when we can connect to the cluster using sqlcmd.

The attached script [run_3_node_cluster.sh](run_3_node_cluster.sh) incorporates the commands above and can be used or modified for testing purposes.

In a similar way, it is also possible to test VoltDB features that involve multiple clusters. You just need to ensure that each instance uses different ports and a different directory. It may help to set a lower sitesperhost value in the deployment.xml file, so that each instance doesn't create as many threads and use as many resources as the default settings.
