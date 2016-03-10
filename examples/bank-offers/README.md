# VoltDB Bank Offers Example App

Use Case
--------
Ingest generated consumer purchase transaction data and find the best matching offer to present to the consumer at the point of sale.

Matching offers are found using a query that joins a summary view of the account activity with this vendor (merchant) against the available offers from that vendor.  The best match is determined by the priority for the offers that was set by the vendor.

The result is inserted into a persistent table, where it could drive a live web interaction, such as serving an online ad that describes the offer, retrieved from the inputs of the account and vendor_id.  It is also shown in the web dashboard as recent examples of offers.  This result is also inserted into an export table so that it can be pushed to Hadoop using the HTTP export connector.

The web dashboard shows recent offers as well as a moving chart of offers/sec and overall transactions/sec.


Code organization
-----------------
The code is divided into projects:

- "db": the database project, which contains the schema, stored procedures and other configurations that are compiled into a catalog and run in a VoltDB database.  
- "client": a java client that generates tick events and records performance metrics.
- "web": a simple web server that provides the demo dashboard.

See below for instructions on running these applications.  For any questions, 
please contact fieldengineering@voltdb.com.

Pre-requisites
--------------

Before running these scripts you need to have VoltDB 4.0 or later installed.  If you choose the .tar.gz file distribution, simply untar it to a directory such as your $HOME directory, then add the bin subdirectory to your PATH environment variable.  For example:

    export PATH="$PATH:$HOME/voltdb-ent-4.7/bin"

You may choose to add this to your .bashrc file.

If you installed the .deb or .rpm distribution, the binaries should already be in your PATH.  To verify this, the following command should return a version number:

    voltdb --version

Single Server Instructions
-----------------

1. Start the web server

    ./run.sh start_web
   
2. Start the database server

    ./run.sh server

Optionally type Ctrl-C to close the tailing of the server log before proceeding, or open a new terminal in the same directory.

3. Start the client

    ./run.sh client

4. Open a web browser to http://hostname:8081

5. To stop the demo:

Stop the client (if it hasn't already completed)

    Ctrl-C
    
Stop the database

    voltadmin shutdown
   
Stop the web server

    ./run.sh stop_web


Options
-------
You can control various characteristics of the demo by modifying the parameters passed into the NbboBenchmark java application in the "client" function within the run.sh script.

Speed & Duration:

    --duration=120                (benchmark duration in seconds)
    --autotune=true               (true = ignore rate limit, run at max throughput until latency is impacted)
                                  (false = run at the specified rate limit)
    --ratelimit=20000             (when autotune=false, run up to this rate of requests/second)
    --custcount=100000            (number of customers to pre-populate)
    --vendorcount=5000            (number of vendors to pre-populate)


Instructions for running on a cluster
-------------------------------------

Before running this demo on a cluster, make the following changes:

1. On each server, edit the run.sh file to set the HOST variable to the name of a selected server in the cluster:
    
    HOST=voltserver01
    
2. On each server, edit db/deployment-cluster.xml to change hostcount to the actual number of servers (it is defaulted to 3):

    <cluster hostcount="3" sitesperhost="8" kfactor="1" />

4. On each server, start the database. Start with the same server as "$HOST" that you set above, e.g. voltserver01

	./run.sh cluster_server

On each of the servers you should see the following output:

    Host id of this node is: 0
    
When all servers have joined the cluster and it becomes available you will see:

    Server completed initialization.
    
5. To run the client, edit the run.sh script on the server where you intend to run the client to set the SERVERS variable to a comma-separated list of the servers in the cluster (note: this could be a separate server or one of the servers in the cluster).

    SERVERS=voltserver01,voltserver02,voltserver03
    
6. Run the client script:

	./run.sh client



Instructions for exporting to CSV
---------------------------------
1. Edit the deployment.xml file to add the following.  For an example, see the provided deployment-export-csv.xml file.

```xml
<export enabled="true" target="file">  
 <configuration>  
  <property name="type">csv</property>  
  <property name="nonce">MyExport</property>  
 </configuration>
</export>
```

2. Then follow the instructions for running on a single server or cluster.


Instructions for exporting to Hadoop
------------------------------------
1. Edit the deployment.xml file to add the following.  See the provided deployment-export-hortonworks.xml and deployment-export-cloudera.xml files.

```xml
  <export enabled="true" target="http">
    <configuration>
      <property name="endpoint">http://sandbox.hortonworks.com:50070/webhdfs/v1/%t/data%p-%g.%t.csv</property>
      <property name="type">csv</property>
      <property name="batch.mode">true</properoty>
      <property name="period">120</property>
    </configuration>
  </export>
```

or 

```xml
  <export enabled="true" target="http">
     <configuration>
       <property name="endpoint">http://quickstart.cloudera:50070/webhdfs/v1/user/cloudera/%t/data%p-%g.%t.csv?user.name=cloudera</property>
       <property name="type">csv</property>
     </configuration>
   </export>
```


2. Then follow the instructions for running on a single server or cluster.

