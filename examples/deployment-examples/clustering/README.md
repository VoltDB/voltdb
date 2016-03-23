How to Run VoltDB Examples in a Cluster
---------------------------------------

**Step One: Create a Deployment File**

**Step Two: Prepare the Client to Connect to Multiple 



**Step Two: Choose a Leader**

**Step Three: Choose a Leader**


1. On each server, edit the run.sh file to set the STARTUPLEADERHOST variable to the name of the **first** server in the cluster:

   `STARTUPLEADERHOST=voltserver01`

   Note. It doesn't matter which host in a cluster you pick to be the leader, so long as it's the same value across all servers.

2. On each server, edit db/deployment-cluster.xml to set hostcount to the correct number of servers:

    <cluster hostcount="3" sitesperhost="8" kfactor="0" />

Also, in the same file, set the voltdbroot path to an appropriate path for your servers.

4. On each server, run the start script:

    ./start_db_cluster.sh

5. On one server, Edit the run_client.sh script to set the SERVERS variable to a comma-separated list of the servers in the cluster

    SERVERS=voltserver01,voltserver02,voltserver03

6. Run the client script:

    ./run_client.sh
