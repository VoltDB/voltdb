XDCR Guide
==========

This is a tutorial about how to start up two or more XDCR clusters (3 nodes each in this example), then do various maintenance tasks such as restoring from snapshot, and monitor some important details about the replication status.

You should begin by reading [Using Cross Datacenter Replication](https://docs.voltdb.com/UsingVoltDB/DbRepHowToActive.php) from Using VoltDB. As this mentions, the clusters should be started one at a time. If there is any data that needs to be loaded from a snapshot, it is best to load it into the first cluster before starting the second cluster, so that the clusters can use snapshots to make the initial transfer the bulk of data. This initial transfer of data via snapshot is also not a fault-tolerant process, so this tutorial provides some additional tips on how to monitor the progress and verify that the clusters reach the point where they are fault tolerant.

This tutorial was done in version 7.9.1 and should work with all future versions. If you are using an older version, you should consult the VoltDB docs, as some attributes and commands might not be available.


Starting up the Clusters
------------------------

### 1) Configuring the deployment files:

Put the necessary <dr> code in the deployment_1_xdcr.xml file, below the deployment tag and above the cluster tag:

    <deployment>
      <dr id="1" role="xdcr">
        <connection source="node4,node5,node6" />
      </dr>
      <cluster sitesperhost="4" kfactor="1"/>
      ...


This file will be used by the first cluster of 3 nodes.

Copy this file and replace <dr id="1" with <dr id="2" and save it as deployment_2_xdcr.xml.

    <deployment>
      <dr id="2" role="xdcr">
        <connection source="node1,node2,node3" />
      </dr>
      <cluster sitesperhost="4" kfactor="1"/>
      ...

Both of these files are available in the deployment-file-examples directory.

The two clusters are differentiated with this id attribute. Notice how each cluster is assigned the "xdcr" role as well. This role name is case-sensitive.

The connection source attribute is the unique hostnames or IP addresses of the opposite cluster. So node1, node2, and node3 use node4, node5, and node6, and vice versa. The nodes will use the default replication port 5555 unless otherwise stated using the syntax "node4:5560", for example.

Sitesperhost and kfactor can be changed as desired.

### 2) Initializing the nodes:
Use the following commands to initialize the nodes.
For Cluster 1 (node1, node2, and node3), use:

    voltdb init --force --config=deployment_1.xml

For Cluster 2 (node_4, node_5, and node_6), use:

    voltdb init --force --config=deployment_2.xml

### 3) Starting Cluster 1, loading schema and data:
The clusters should be started one at a time. Start the first three nodes with the following command:

    voltdb start --host="node1,node2,node3"

Once Cluster 1 is fully started, load the schema with:

    sqlcmd < ddl.sql

Make sure your schema has at least one DR table, specified like so:

    DR TABLE table_name_here;

Then, make sure this node is loaded with data. You can use the voltdb restore command to load a snapshot, or run csvloader or other tools to populate data. If you have any pre-existing data, it is very important in starting up XDCR clusters that the first cluster is loaded with both schema and pre-existing data, even before the second cluster starts at all. This best ensures a smooth initial copying of the data to any joining XDCR cluster.

### 4) Starting Cluster 2:

After this is done, start Cluster 2 by running the following command on each node:

    voltdb start --host="node4,node5,node6"

At this point, Both clusters are running, but since the second cluster does not yet have a schema, replication has not yet started. You can see this if you call the following on each cluster:

    exec @Statistics DRROLE 0;
    ROLE  STATE    REMOTE_CLUSTER_ID
    ----- -------- ------------------
    XDCR  PENDING                  -1


Now, load the schema by running the following command:

    sqlcmd < ddl.sql

When the matching schema (including DR TABLE definitions) is loaded on the second cluster, XDCR replication will begin to initialize.

At this point, all data should be copying over to the second cluster.

You can check the status by calling @Statistics. As this example shows, initially Cluster 1 is sending a snapshot to Cluster 2. The DRPRODUCER Statistics STREAMTYPE is SNAPSHOT_SYNC, indicating it is streaming a snapshot to the other cluster.

    exec @Statistics DRROLE 0;
    ROLE  STATE   REMOTE_CLUSTER_ID
    ----- ------- ------------------
    XDCR  ACTIVE                   2

    exec @Statistics DRPRODUCER 0;
    TIMESTAMP      HOST_ID  HOSTNAME     CLUSTER_ID  REMOTE_CLUSTER_ID  PARTITION_ID  STREAMTYPE     TOTALBYTES  TOTALBYTESINMEMORY  TOTALBUFFERS  LASTQUEUEDDRID  LASTACKDRID  LASTQUEUEDTIMESTAMP         LASTACKTIMESTAMP            ISSYNCED  MODE    QUEUE_GAP
    -------------- -------- ------------ ----------- ------------------ ------------- -------------- ----------- ------------------- ------------- --------------- ------------ --------------------------- --------------------------- --------- ------- ----------
     1548267111772        0         ...            1                  2             1 SNAPSHOT_SYNC     10648691            10648691            40         1013666       262155 1970-01-01 00:00:00.000000  1970-01-01 00:00:00.000000  true      NORMAL       52430
     1548267111772        0         ...            1                  2             2 SNAPSHOT_SYNC     10639719            10639719            40          978712       227201 1970-01-01 00:00:00.000000  1970-01-01 00:00:00.000000  true      NORMAL       52430
     ...

    TIMESTAMP      HOST_ID  HOSTNAME     CLUSTER_ID  REMOTE_CLUSTER_ID  STATE   SYNCSNAPSHOTSTATE      ROWSINSYNCSNAPSHOT  ROWSACKEDFORSYNCSNAPSHOT  QUEUEDEPTH
    -------------- -------- ------------ ----------- ------------------ ------- ---------------------- ------------------- ------------------------- -----------
     1548267111774        0         ...            1                  2 ACTIVE  SENDING_SYNC_SNAPSHOT              3006044                    961551           0
     ...

    exec @Statistics DRCONSUMER 0;
    TIMESTAMP      HOST_ID  HOSTNAME                  CLUSTER_ID  REMOTE_CLUSTER_ID  STATE    REPLICATION_RATE_1M  REPLICATION_RATE_5M
    -------------- -------- ------------------------- ----------- ------------------ -------- -------------------- --------------------
     1548267113522        0         .../10.10.182.51            1                  2 RECEIVE                    23                   23
     ...

    TIMESTAMP      HOST_ID  HOSTNAME                  CLUSTER_ID  REMOTE_CLUSTER_ID  PARTITION_ID  IS_COVERED  COVERING_HOST      LAST_RECEIVED_TIMESTAMP     LAST_APPLIED_TIMESTAMP      IS_PAUSED
    -------------- -------- ------------------------- ----------- ------------------ ------------- ----------- ------------------ --------------------------- --------------------------- ----------
     1548267113522        0         .../10.10.182.51            1                  2             0 true        10.10.182.51:5575  2019-01-23 18:11:45.108000  2019-01-23 18:11:45.108000  false
     1548267113522        0         .../10.10.182.51            1                  2             2 true        10.10.182.51:5575  2019-01-23 18:11:45.108000  2019-01-23 18:11:45.108000  false
     ...

Moments later, after the sync snapshot has been loaded, you can check @Statistics on Cluster 1 to see that the cluster has is now streaming transactions. DRPRODUCER Statistics shows STREAMTYPE = TRANSACTIONS. At this point, each cluster is sending a transaction stream which keeps track of which transactions have been applied and can fail over or resume in the event of node or cluster failures.

    exec @Statistics DRROLE 0;
    ROLE  STATE   REMOTE_CLUSTER_ID
    ----- ------- ------------------
    XDCR  ACTIVE                   2

    exec @Statistics DRPRODUCER 0;
    TIMESTAMP      HOST_ID  HOSTNAME     CLUSTER_ID  REMOTE_CLUSTER_ID  PARTITION_ID  STREAMTYPE    TOTALBYTES  TOTALBYTESINMEMORY  TOTALBUFFERS  LASTQUEUEDDRID  LASTACKDRID  LASTQUEUEDTIMESTAMP         LASTACKTIMESTAMP            ISSYNCED  MODE    QUEUE_GAP
    -------------- -------- ------------ ----------- ------------------ ------------- ------------- ----------- ------------------- ------------- --------------- ------------ --------------------------- --------------------------- --------- ------- ----------
     1548267138110        0         ...            1                  2             0 TRANSACTIONS            0                   0             0              -1            0 2008-01-01 00:00:00.000000  2008-01-01 00:00:00.000000  true      NORMAL           0
     1548267138110        0         ...            1                  2             1 TRANSACTIONS            0                   0             0              -1            0 2008-01-01 00:00:00.000000  2008-01-01 00:00:00.000000  true      NORMAL           0
     ...

    TIMESTAMP      HOST_ID  HOSTNAME     CLUSTER_ID  REMOTE_CLUSTER_ID  STATE   SYNCSNAPSHOTSTATE  ROWSINSYNCSNAPSHOT  ROWSACKEDFORSYNCSNAPSHOT  QUEUEDEPTH
    -------------- -------- ------------ ----------- ------------------ ------- ------------------ ------------------- ------------------------- -----------
     1548267138110        0         ...            1                  2 ACTIVE  NONE                              NULL                         0           0
     ...

    exec @Statistics DRCONSUMER 0;
    TIMESTAMP      HOST_ID  HOSTNAME                  CLUSTER_ID  REMOTE_CLUSTER_ID  STATE    REPLICATION_RATE_1M  REPLICATION_RATE_5M
    -------------- -------- ------------------------- ----------- ------------------ -------- -------------------- --------------------
     1548267138794        0         .../10.10.182.51            1                  2 RECEIVE                    15                   21
     ...

    TIMESTAMP      HOST_ID  HOSTNAME                  CLUSTER_ID  REMOTE_CLUSTER_ID  PARTITION_ID  IS_COVERED  COVERING_HOST      LAST_RECEIVED_TIMESTAMP     LAST_APPLIED_TIMESTAMP      IS_PAUSED
    -------------- -------- ------------------------- ----------- ------------------ ------------- ----------- ------------------ --------------------------- --------------------------- ----------
     1548267138794        0         .../10.10.182.51            1                  2             0 true        10.10.182.51:5575  2019-01-23 18:11:45.108000  2019-01-23 18:11:45.108000  false
     1548267138794        0         .../10.10.182.51            1                  2             2 true        10.10.182.51:5575  2019-01-23 18:11:45.108000  2019-01-23 18:11:45.108000  false
     ...


Cluster 2 Statistics will also show it is in an ACTIVE state and using STREAMTYPE = TRANSACTIONS:

    exec @Statistics DRROLE 0;
    ROLE  STATE   REMOTE_CLUSTER_ID
    ----- ------- ------------------
    XDCR  ACTIVE                   1

    exec @Statistics DRPRODUCER 0;
    TIMESTAMP      HOST_ID  HOSTNAME     CLUSTER_ID  REMOTE_CLUSTER_ID  PARTITION_ID  STREAMTYPE    TOTALBYTES  TOTALBYTESINMEMORY  TOTALBUFFERS  LASTQUEUEDDRID  LASTACKDRID  LASTQUEUEDTIMESTAMP         LASTACKTIMESTAMP            ISSYNCED  MODE    QUEUE_GAP
    -------------- -------- ------------ ----------- ------------------ ------------- ------------- ----------- ------------------- ------------- --------------- ------------ --------------------------- --------------------------- --------- ------- ----------
     1548267204214        0         ...            2                  1             0 TRANSACTIONS            0                   0             0               0            0 2019-01-23 18:11:45.108000  2019-01-23 18:11:45.108000  true      NORMAL           0
     1548267204214        0         ...            2                  1             1 TRANSACTIONS            0                   0             0               0            0 2019-01-23 18:11:45.108000  2019-01-23 18:11:45.108000  true      NORMAL           0
     ...

    TIMESTAMP      HOST_ID  HOSTNAME     CLUSTER_ID  REMOTE_CLUSTER_ID  STATE   SYNCSNAPSHOTSTATE  ROWSINSYNCSNAPSHOT  ROWSACKEDFORSYNCSNAPSHOT  QUEUEDEPTH
    -------------- -------- ------------ ----------- ------------------ ------- ------------------ ------------------- ------------------------- -----------
     1548267204216        0         ...            2                  1 ACTIVE  NONE                              NULL                         0           0
     ...

    exec @Statistics DRCONSUMER 0;
    TIMESTAMP      HOST_ID  HOSTNAME                  CLUSTER_ID  REMOTE_CLUSTER_ID  STATE    REPLICATION_RATE_1M  REPLICATION_RATE_5M
    -------------- -------- ------------------------- ----------- ------------------ -------- -------------------- --------------------
     1548267204937        0 Ballard-MBP/10.10.182.51            2                  1 RECEIVE                290944               662539
     ...

    TIMESTAMP      HOST_ID  HOSTNAME                  CLUSTER_ID  REMOTE_CLUSTER_ID  PARTITION_ID  IS_COVERED  COVERING_HOST      LAST_RECEIVED_TIMESTAMP     LAST_APPLIED_TIMESTAMP      IS_PAUSED
    -------------- -------- ------------------------- ----------- ------------------ ------------- ----------- ------------------ --------------------------- --------------------------- ----------
     1548267204937        0         .../10.10.182.51            2                  1             0 true        10.10.182.51:5555  1970-01-01 00:00:00.000000  1970-01-01 00:00:00.000000  false
     1548267204937        0         .../10.10.182.51            2                  1             2 true        10.10.182.51:5565  1970-01-01 00:00:00.000000  1970-01-01 00:00:00.000000  false
     ...


Your two XDCR clusters are complete and actively replicating transactions.


### 5) Starting additional clusters

If you have any additional clusters, they can be configured and started in the same way as Cluster 2 above, but they should not be started until replication between Cluster 1 and Cluster 2 is fully established as shown above. Note, as mentioned in Using VoltDB: [11.3 Using Cross Datacenter Replication](https://docs.voltdb.com/UsingVoltDB/DbRepHowToActive.php#DbRepActiveStartDbs):

    Only one database can join the XDCR network at a time. You must wait for each joining cluster to complete the initial synchronization before starting the next.


Restarting the Clusters from Snapshot
-------------------------------------

If you shut the clusters down and wish to restart them without changing the configuration file or schema, then you can restart them with the same "voltdb start" commands used to start them initially:

    voltdb start --host="node1,node2,node3"

or

    voltdb start --host="node4,node5,node6"

As you did when first creating the XDCR cluster. The two node clusters will use the command logs and dr_overflow files to recreate the data and start following each other again.

However, if you need to make a change to the configuration file or schema that cannot be done while the clusters are running, you may need to stop the cluster and restore from snapshot.

First, create a directory to use as a place to store the snapshot files. The directory needs to exist on all the nodes. It can be saved like this:

    voltadmin save ~/restore snapshot_1

Then, shutdown the clusters and make changes to the configuration file or schema. Re-initialize the nodes with force:

    voltdb init --force --config=deployment_1.xml
    voltdb init --force --config=deployment_2.xml

Start up the first cluster and reload the schema:

    sqlcmd < ddl.sql

Note that if the schema changes too much, such as deleting tables, the snapshot might not restore correctly.
Finally, use the following command to restore from snapshot:

    voltadmin restore ~/restore snapshot_1

Once this completes and is successful, restart the second cluster and reload its schema with sqlcmd < ddl.sql. Your two XDCR clusters should be ready and copying each other as normal with their new configuration file and/or schema.


When is does replication become fault-tolerant?
-----------------------------------------------

It's important to know what status the XDCR clusters are in, because until replication is fully established, it may not be fault tolerant and may require restarting clusters in order to resume replication in the event of a failure. Once replication is established, it can tolerate node failures on either the master or replica clusters, as well as cluster failures and restarts. This is because the transactional streams keep track of redundant copies and which transactions have been applied. However, when XDCR clusters that are first joining the mesh, they get a copy of the existing data in the form of a snapshot. The process of saving, copying and loading the snapshot is not fault tolerant. It assumes all the participating nodes will continue running until the snapshot is fully loaded. If a failure occurs during this initialization process, replication cannot just resume from where it left off, it must be started again as if the clusters are being joined together for the first time.

A failure during snapshot the synchronization process produces an ERROR message in the volt.log file, for example:

    ERROR: DR Subsystem could not be recovered because the Sync Snapshot did not complete successfully. Please restart the cluster in CREATE mode and with the DR connection source of an existing cluster.

The cluster that gives you this error message must be shut down, and the remaining cluster with the snapshot data must have its data replication reset, using the following command (note that "--all" can be replaced with the appropriate cluster id, which in this case would be 2):

    voltadmin dr reset --all

Once that happens, the second cluster can be re-initiated with force:

    voltdb init --force --config=deployment_2.xml

And reloaded with the appropriate schema:

    sqlcmd < ddl.sql

This should restart the replication process, but the XDCR clusters will again need to successfully complete the snapshot synchronization process before they will be fault tolerant. When the clusters start transmitting transaction data will the replication be stable. You can monitor what data is being transmitted using sqlcmd, followed by @Statistics DRPRODUCER:

    exec @Statistics DRPRODUCER 0;

This will show two tables of data, but the important column will be STREAMTYPE. It will be either TRANSACTIONS or SNAPSHOT. If it says SNAPSHOT, then the clusters are still in the snapshot synchronization process. Once they show TRANSACTIONS, the replication stream is fully established and fault tolerant.


Monitoring the Clusters
-----------------------


This section is dedicated to monitoring the states of your XDCR clusters, since as mentioned before, it is critical to safely timing certain modifications to your mesh, including adding more XDCR clusters.

Using the @Statistics component DRPRODUCER is the best place to start when monitoring XDCR clusters. The following is what you will see upon starting two 1 node clusters, both set up with a deployment file indicating XDCR and pointing to each other as connection sources (sitesperhost=1 and kfactor=0). They also point to a third cluster as a connection source, as we will be creating a 3 way XDCR mesh.

An example of what the DR section of the Cluster 1's deployment file might look like is below:

    <dr id="1" role="xdcr">
        <connection source="server_2,server_3" />
    </dr>


The following is what DRPRODUCER will look like if Cluster_ID 1 has its schema and a few rows of data loaded, but Cluster_ID 2 does not have a schema loaded:

##### DRPRODUCER CLUSTER 1


    CLUSTER_ID  REMOTE_CLUSTER_ID  STATE    SYNCSNAPSHOTSTATE  ROWSINSYNCSNAPSHOT  ROWSACKEDFORSYNCSNAPSHOT  QUEUEDEPTH
    ----------- ------------------ -------- ------------------ ------------------- ------------------------- -----------
              1                 -1 PENDING  NONE                              NULL                         0           0


##### DRPRODUCER CLUSTER 2


    CLUSTER_ID  REMOTE_CLUSTER_ID  STATE    SYNCSNAPSHOTSTATE  ROWSINSYNCSNAPSHOT  ROWSACKEDFORSYNCSNAPSHOT  QUEUEDEPTH
    ----------- ------------------ -------- ------------------ ------------------- ------------------------- -----------
              2                 -1 PENDING  NONE                              NULL                         0           0


Upon loading the same schema into Cluster_ID 2, the DRPRODUCER changes to this:

##### DRPRODUCER CLUSTER 1


    CLUSTER_ID  REMOTE_CLUSTER_ID  PARTITION_ID  STREAMTYPE
    ----------- ------------------ ------------- -------------
              1                  2             0 TRANSACTIONS
              1                  2         16383 TRANSACTIONS

    CLUSTER_ID  REMOTE_CLUSTER_ID  STATE   SYNCSNAPSHOTSTATE  ROWSINSYNCSNAPSHOT  ROWSACKEDFORSYNCSNAPSHOT  QUEUEDEPTH
    ----------- ------------------ ------- ------------------ ------------------- ------------------------- -----------
              1                  2 ACTIVE  NONE                              NULL                         0           0


##### DRPRODUCER CLUSTER 2


    CLUSTER_ID  REMOTE_CLUSTER_ID  PARTITION_ID  STREAMTYPE
    ----------- ------------------ ------------- -------------
              2                  1             0 TRANSACTIONS
              2                  1         16383 TRANSACTIONS


    CLUSTER_ID  REMOTE_CLUSTER_ID  STATE   SYNCSNAPSHOTSTATE  ROWSINSYNCSNAPSHOT  ROWSACKEDFORSYNCSNAPSHOT  QUEUEDEPTH
    ----------- ------------------ ------- ------------------ ------------------- ------------------------- -----------
              2                  1 ACTIVE  NONE                              NULL                         0           0


Once the two clusters are communicating with each other, as you can see with the corresponding REMOTE_CLUSTER_IDs, you must also ensure that the streamtype is TRANSACTIONS (if it says SNAPSHOT, it is not ready, see the previous section) and the STATE is ACTIVE. Only then can you add a new XDCR cluster to the mesh. The third cluster, after starting but before loading any schema, will show the following DRPRODUCER statistics:

##### DRPRODUCER CLUSTER 3


    CLUSTER_ID  REMOTE_CLUSTER_ID  STATE    SYNCSNAPSHOTSTATE  ROWSINSYNCSNAPSHOT  ROWSACKEDFORSYNCSNAPSHOT  QUEUEDEPTH
    ----------- ------------------ -------- ------------------ ------------------- ------------------------- -----------
              3                 -1 PENDING  NONE                              NULL                         0           0


If the third cluster shows anything different from this, it is not ready to join the mesh. It probably already has a schema or data inside it. Make sure the third cluster is starting up empty. Once you load the appropriate schema into cluster 3, it will show the following:

##### DRPRODUCER CLUSTER 3


    CLUSTER_ID  REMOTE_CLUSTER_ID  STATE   SYNCSNAPSHOTSTATE  ROWSINSYNCSNAPSHOT  ROWSACKEDFORSYNCSNAPSHOT  QUEUEDEPTH
    ----------- ------------------ ------- ------------------ ------------------- ------------------------- -----------
              3                  1 ACTIVE  NONE                              NULL                         0           0
              3                  2 ACTIVE  NONE                              NULL                         0           0


Once you see the other two clusters shown in REMOTE_CLUSTER_ID and the STATE as ACTIVE, the 3 cluster XDCR mesh is complete.
