# How to Run VoltDB Examples in a Cluster

### Step One: Create a Deployment File

In order to run VoltDB in a cluster, you're going to need to initialize the server with a deployment file.

When you initialize without one, the server uses a default 1-node deployment file and writes it out to the voltdbroot folder. If you've already run a VoltDB example, you can probably find this default file there. It should have the following contents:

```xml
<?xml version="1.0"?>
<deployment>
   <cluster hostcount="1" />
   <httpd enabled="true">
      <jsonapi enabled="true" />
   </httpd>
</deployment>
```

So copy that file up one level, or simply create a file named `deployment.xml` with the contents above. You can even borrow a file from the `/examples/HOWTOs/deployment-file-examples` directory.

To start with a deployment file add `-C path/to/deployment.xml` to the VoltDB startup commands. For example:

```bash
voltdb init --force -C deployment.xml
#
# start a 3 node cluster comprised of server1,server2,server3
voltdb start -c 3 -H server1,server2
```

### Step Two: Prepare the Client to Connect to Multiple Servers

Make a list of the hostnames of the servers on which you're going to run VoltDB.

For the purposes of this example, let's say they are `voltserver1`, `voltserver2`, and `voltserver3`.

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

### Step Two: Choose a set of Coordinator Nodes

Consider our host list again: `voltserver1`, `voltserver2`, and `voltserver3`.

We need to pick a set of coordinator nodes. These are the node the other nodes will connect to at startup. It can be any node in the list. After startup, the coordinators have no special role at all. After startup, there are no special nodes in a VoltDB cluster.

Let's pick `voltserver1, voltserver2` for this example.

### Step Three: Start VoltDB

On `voltserver1` type:

```bash
> voltdb init --force -C deployment
> voltdb start -c 3 -H voltserver1,voltserver2
```

On `voltserver2` type:

```bash
> voltdb init --force -C deployment
> voltdb start -c 3 -H voltserver1,voltserver2
```

On `voltserver3` type:

```bash
> voltdb init --force -C deployment
> voltdb start -c 3 -H voltserver1,voltserver2
```

Now you should have a running cluster. On all three machines you should see the `Server Completed Initialization.` log message.

Note that you can actually specify the deployment on all three nodes, the deployment files may differ only in the following elements: paths, host count, and admin mode (the last two are ignored).

### Step Four: Run the Client

Run the client script:

    ./run.sh client
