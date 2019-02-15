# How to Run VoltDB Examples in a Cluster

### Step One: Initialize voltdbroots on all the cluster nodes
VoltDB requires you to initialize a root directory before starting a new cluster. 
If you want to run with no data redundancy (kfactor=0) you do not need a configuration file.

You can just go to each node and type:

```bash
voltdb init --force
```

If you want to run with data redundancy you will need a configuration file that specifies
the redundancy. It can be as simple as this

```xml
<?xml version="1.0"?>
<deployment>
   <cluster kfactor="1"/>
   <httpd enabled="true">
      <jsonapi enabled="true" />
   </httpd>
</deployment>
```

Save this file as config.xml, or use the HOWTOS/deployment-file-examples/deployment-with-redundancy.xml file provided in the VoltDB kit.

To initialize with a config file, type this on all nodes: 
```bash
voltdb init --force --config=config.xml
```
### Step Two: Start VoltDB

To start a 3 node cluster you will provide the server count using --count= or -c. On your 3 servers, voltserver1, voltserver2, voltserver3 issue the same command:

```bash
> voltdb start -c 3 -H voltserver1,voltserver2,voltserver3
```

Now you should have a running cluster. On all three machines you should see the `Server Completed Initialization.` log message.


### Step Three: Prepare the Client run.sh script to Connect to Multiple Servers

Make a list of the hostnames of the servers on which you're going to run VoltDB.

For the purposes of this example, we will use 3 servers: `voltserver1`, `voltserver2`, and `voltserver3`.

On any machines where you're going to run the client, you're going to need to edit the `run.sh` file.

Find the line that says:

```
SERVERS="localhost"
```

And change it to say:

```
SERVERS="voltserver1,voltserver2,voltserver3"
```

Note that this is equivalent to:

```
SERVERS="voltserver1:21212,voltserver2:21212,voltserver3:21212"
```

If your servers are configured with different ports, you would need to change this, but the defaults should be fine for almost all cases.

Now save the `run.sh` file.


### Step Four: Run the Client

Run the client script:

    ./run.sh client
