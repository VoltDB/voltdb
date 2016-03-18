Instructions for running on a cluster
-------------------------------------

Before running this demo on a cluster, make the following changes:

1. On each server, edit the start_db_cluster.sh file to set the HOST variable to the name of the **first** server in the cluster:
    
    HOST=voltserver01
    
2. On each server, edit db/deployment-cluster.xml to set hostcount to the correct number of servers:

    <cluster hostcount="3" sitesperhost="8" kfactor="0" />
	
Also, in the same file, set the voltdbroot path to an appropriate path for your servers.

4. On each server, run the start script:

    ./start_db_cluster.sh
    
5. On one server, Edit the run_client.sh script to set the SERVERS variable to a comma-separated list of the servers in the cluster

    SERVERS=voltserver01,voltserver02,voltserver03
    
6. Run the client script:

    ./run_client.sh
