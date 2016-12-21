# VoltDB Binding for YCSB

This section describes how to run YCSB (Yahoo Cloud Serving Benchmark) on VoltDB.

Set Up YCSB
-------------------
First you need to download YCSB, which may be done as follows:

		wget https://github.com/brianfrankcooper/YCSB/releases/download/0.10.0/ycsb-0.10.0.tar.gz
		tar -xfvz ycsb-0.10.0.tar.gz

If you want to run YCSB with customized zipfian distribution workload, you can instead do:

		wget https://github.com/xinjiacs/ycsb-0.1.4/releases/download/ycsb-0.1.4.1/ycsb-0.1.4-zipfian.tar.gz
		tar -xf ycsb-0.1.4-zipfian.tar.gz

Next, you need to set:

		export YCSB_HOME="<directory where you extracted the above tarball>"

Start VoltDB Server
--------------------
To start or add a server to your cluster, invoke run.sh with the "server" parameter, passing the name of the leader host as the second parameter:

		./run.sh server [leader]

For example, to start a cluster with 3 nodes (host0(leader), host1, host2), set hostcount="3" in deployment.xml. Then type the following command on those hosts:

		./run.sh server host0

If leader host is not provided, by default "localhost" is used.

You can also specify other server configurations such as hostcount, sitesperhost or kfactor in deployment.xml.

Lastly, let VoltDB load the schema and stored procedures:

		./run.sh init

Run Workload
--------------------
First, add all VoltDB server nodes in your cluster to "voltdb.servers" in base.properties. In this file, you may also tune other parameters such as the number of client threads to use, and the number and size of fields for each key.

Then, before running the workload, you need to "load" data first:

		./run.sh load

You can adjust loading parameters such as the number of keys to insert in load.properties.

Now you can run a workload by passing the name of the workload file following the "workload" parameter. For example, to run workload A:

		./run.sh workload workloada

If no file name is provided, "workload" uses the YCSB core "workloadb" which mixes 95% reads with 5% updates. Please make sure specified workload file exists in $YCSB_HOME/workloads.

The zipfian distribution workload parameter is set as key with a double value greater than 0, by default is 0.99: zipfianconstant.

Other run.sh Actions
---------------------
- *run.sh* : start the server on localhost
- *run.sh server [leader]* : start or add the server to cluster. Default: localhost
- *run.sh srccompile* : compile java clients and stored procedures
- *run.sh jars* : compile java clients and stored procedures into two Java jarfiles
- *run.sh init* : load the schema and stored procedures to server
- *run.sh load* : load YCSB data to server
- *run.sh workload [file]* : run a workload. Default: workloadb
- *run.sh clean* : remove compilation and runtime artifacts, as well as the jars
- *run.sh help* : show help messages