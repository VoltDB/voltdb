XDCR Guide
====================


This is a tutorial about how to start up two XDCR 3 node clusters, do various maintenance tasks such as restoring from snapshot, and monitor some important details about the replication status.
This tutorial was done in version 7.9.1 and should work with all future versions. If you are using an older version, you should consult the VoltDB docs, as some attributes and commands might not be available.


Starting up the Clusters
====================

###1) Making the deployment files:

Put the necessary <dr> code in the deployment_1_xdcr.xml file, below the deployment tag and above the cluster tag:

    <deployment>

      <dr id="1" role="xdcr">
      	<connection source="node4,node5,node6" />  
      </dr>

    	<cluster sitesperhost="4" kfactor="1"/>


This file will be used by the first cluster of 3 nodes. Copy this file and replace <dr id="1" with <dr id="2" and save it as deployment_2_xdcr.xml. Both of these files are available in the deployment-file-examples directory.

The two clusters are differentiated with this id attribute. Notice how each cluster is assigned the xdcr role as well. 

The connection source attribute is the unique hostnames or IP addresses of the opposite cluster. So node1, node2, and node3 use node4, node5, and node6, and vice versa. The nodes will use the default replication port 5555 unless otherwise stated using the syntax "node4:5560", for example.

Sitesperhost and kfactor can be changed as desired. 

###2) Initializing the nodes:
Use the following commands to initialize the nodes.
For node1, node2, and node3, use:


		voltdb init --force --config=deployment_1.xml

For node_4, node_5, and node_6, use:

		voltdb init --force --config=deployment_2.xml

###3) Starting the first cluster:
The two clusters should be started separately. Start the first three nodes with the following code:


		voltdb start --host="node1,node2,node3"

Once the cluster is fully started, load the schema with:

		sqlcmd < ddl.sql

Make sure your schema has at least one DR table, specified like so:

		DR TABLE table_name_here;

Then, make sure this node is loaded with data. It is very important in starting up xdcr clusters that the first cluster is loaded with both schema and pre-existing data, even before the second cluster starts at all. This best ensures a smooth initial copying of the data to any joining xdcr cluster. After this is done, start the second cluster and load the schema.

At this point, all data should have already copied over to the second cluster. Your two xdcr clusters are complete and ready to receive transactions.     


Restarting the Clusters from Snapshot
====================

If you shut the clusters down and wish to restart them without changing the configuration file or schema, then you can simply do

		voltdb start --host="node1,node2,node3"
		voltdb start --host="node4,node5,node6"

As you did when first creating the xdcr cluster. The two node clusters will use the command logs and dr_overflow files to recreate the data and start following each other again.


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

Once this completes and is successful, restart the second cluster and reload its schema with sqlcmd < ddl.sql. Your two xdcr clusters should be ready and copying each other as normal with their new configuration file and/or schema.


Monitoring the Clusters
====================

It's important to know what status the XDCR clusters are in, because their resiliency depends on what they are doing, especially in regards to snapshots. XDCR clusters that are streaming snapshot data are particularly vulnerable; any nodes on any participating cluster going down during this time will halt the replication process entirely. The way you know this happened is when an XDCR cluster gives you this error message:

		ERROR: DR Subsystem could not be recovered because the Sync Snapshot did not complete successfully. Please restart the cluster in CREATE mode and with the DR connection source of an existing cluster.

The cluster that gives you this error message must be shut down, and the remaining cluster with the snapshot data must have its data replication reset, using the following command (note that "--all" can be replaced with the appropriate cluster id, which in this case would be 2):

		voltadmin dr reset --all
		
Once that happens, the second cluster can be re-initiated with force:

		voltdb init --force --config=deployment_2.xml

And reloaded with the appropriate schema:

		sqlcmd < ddl.sql
		
This should restart the replication process, but the XDCR clusters will still be vulnerable because the first cluster will be transmitting snapshot data to the second. Only when the clusters start transmitting transaction data will the replication be stable. You can monitor what data is being transmitted using sqlcmd, followed by @Statistics DRPRODUCER:

		exec @Statistics DRPRODUCER 0;
		
This will show two tables of data, but the important column will be STREAMTYPE. It will either be TRANSACTIONS or SNAPSHOT. If it says SNAPSHOT, then the clusters are still vulnerable to replication being halted. It is imperative that no nodes go down during this time.
		

