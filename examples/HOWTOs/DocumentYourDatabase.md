# Documenting your VoltDB deployment
## What you should document when deploying VoltDB

There are various choices that can be made when installing and running VoltDB. It is important to document the choices made for a production deployment. It is also helpful to operators if the choices made are shown in the specific commands used to operate the database. For example, if alternative ports are used, they must be specified on the command line when starting the database. An operator who is restarting a failed node or restarting the cluster after an outage would need to know the right parameters to use for the commands. The VoltDB documentation can show the general commands, but not the specific parameters that should be used for each deployment.

Use this document as a template for an operator "Cheat Sheet", or as a starting point or checklist for more formal project deployment documentation.

## Contents

1. [Provisioning VoltDB Servers](DocumentYourDatabase.md#provisioning-voltdb-servers)
2. [Installing VoltDB](DocumentYourDatabase.md#installing-voltdb)
3. [Creating a database instance](DocumentYourDatabase.md#create-a-database-instance)
4. [Starting the Cluster](DocumentYourDatabase.md#starting-the-cluster)
5. [Loading the Schema](DocumentYourDatabase.md#loading-the-schema)
6. [Stopping the Cluster](DocumentYourDatabase.md#stopping-the-cluster)
7. [Restarting the Cluster](DocumentYourDatabase.md#restarting-the-cluster)
8. [Pause and Resume the Cluster](DocumentYourDatabase.md#pause-and-resume-the-cluster)
9. [Updating the license file](DocumentYourDatabase.md#updating-the-license-file)
10. [Taking a manual backup](DocumentYourDatabase.md#taking-a-manual-backup)
11. [Restoring a manual backup](DocumentYourDatabase.md#restoring-a-manual-backup)
12. [Stopping and restarting the cluster for a maintenance window](DocumentYourDatabase.md#reconfiguring-the-cluster-during-a-maintenance-window)
13. [Rejoining a failed node to the cluster](DocumentYourDatabase.md#rejoining-a-failed-node-to-the-cluster)
14. [Stopping a node manually](DocumentYourDatabase.md#stopping-a-node-manually)
15. [Adding nodes to increase capacity](DocumentYourDatabase.md#adding-nodes-to-increase-capacity)



## Provisioning VoltDB Servers

References:
 - [Using VoltDB: 2.1 Operating System and Software Requirements](http://docs.voltdb.com/UsingVoltDB/ChapGetStarted.php#installReqs) -- lists the minimum hardware requirements as well as the supported OS versions.
 - [Administrator's Guide: 2. Preparing the Servers](https://docs.voltdb.com/AdminGuide/ServerConfigChap.php) -- Checklist and steps to install and configure required software and apply required Linux configuration settings.

### Hardware & OS
You should specify the exact hardware or cloud instance types, so that if it's necessary to provision a replacement or expansion server, it will be the same as the existing servers.

Amazon EC2 Example:

    Production: c4.2xlarge, XX GB of RAM
    Test: c4.2xlarge, XX GB of RAM
    Dev: c4.xlarge, XX GB of RAM
    AMI: Ubuntu 14.04 LTS or 16.04 LTS, RHEL/CentOS 6.6 or 7 or later

Bare-metal example:

    Model: HP ProLiant DL380
    CPUs: 2x Intel Exxxx xx.x GHz (?)-core
    RAM: (?)x  XX GB xxxx speed ECC
    OS: Ubuntu 16.04 LTS


### Software Pre-requisites

**Python**

Most distributions already have Python installed. Verify the version is 2.6 or later (2.7 is recommended).

    python --version


**Java JDK**

Install OpenJDK 8, the full JDK:

    apt-get install openjdk-8-jdk


**NTP**
Note: Server clocks must be within 200 milliseconds for a cluster to start, and ideally should be synchronized within a millisecond. NTP is typically used for this, but some customers use alternatives such as Chrony. This example shows a basic NTP configuration.

For additional guidance on NTP see [Administrator's Guide, 2.5 Configure NTP](https://docs.voltdb.com/AdminGuide/adminserverntp.php) and [Guide to Performance and Customization, Configuring NTP](https://docs.voltdb.com/PerfGuide/ChapNtp.php).

Install NTP

    sudo apt-get install ntp
    sudo service ntp stop

Configure NTP

    sudo vi /etc/ntp.conf

Remove any lines beginning with "server" and add the following lines (this is just an example)

    server time1.google.com
    server time2.google.com
    server time3.google.com

Run the following command repeatedly until the offset is less than +/- 0.001 sec

    sudo ntpdate -b -p 8 time1.google.com

Restart NTP service

    sudo service ntp start

To check that NTP is configured and clocks are closely synchronized:

    ntpq -p host1 host2 host3

**Configure Memory Management**

Follow steps in [Administrator's Guide, 2.3. Configure Memory Management](https://docs.voltdb.com/AdminGuide/adminmemmgt.php)

**Turn off TCP Segmentation**

Follow steps in [Administrator's Guide, 2.4. Turn off TCP Segmentation](https://docs.voltdb.com/AdminGuide/adminservertcpseg.php)


## Installing VoltDB

To install VoltDB, create a user “voltdb” with default permissions and log in with that user to install VoltDB in the home directory:

    cd /home/voltdb
    tar -xzvf LINUX-voltdb-ent-8.3.tar.gz

create a symlink for this version of VoltDB for use in the PATH

    ln -s voltdb-ent-8.3 voltdb

add this to the PATH by adding the following line to .bashrc:

    export PATH=$PATH:$HOME/voltdb/bin

test to ensure you can run the voltdb bin tools

    voltdb --version

### Creating a database instance

Repeat the steps in this section for each server in the cluster.

Make a home folder for the database instance

    mkdir $HOME/voltdb_instance

Make a folder for saved snapshots

    mkdir $HOME/voltdb_saved_snapshots

Copy the DDL.sql, deployment.xml, and license.xml files to the "voltdb_instance" folder.
Checkout from source control the application source code (e.g. procedure .java source files) to this folder.  This includes the DDL.sql, and src directory for stored procedure code.
Copy the deployment.xml and license.xml files into this folder.

On each of the servers (voltserver1, voltserver2, voltserver3), run the following command:

    # on each host
    cd $HOME/voltdb_instance
    voltdb init --force --config deployment.xml --license license.xml

This will create a "voltdbroot" folder in the current directory (voltdb_instance) with subfolders for the persistent disk storage that will be used by the database.

## Starting the Cluster

On each of the servers (voltserver1, voltserver2, voltserver3), run the following command:


    # on each host
    cd $HOME/voltdb_instance
    voltdb start -B -H "voltserver1,voltserver2,voltserver3"

- The -B parameter will start the database as a background daemon, and redirect console outputs to the $HOME/.voltdb_server directory.
- The -H parameter identifies all the hosts in the cluster that the node will connect to. Alternatively, you could just identify a subset of the hosts and use the -c parameter to indicate how many nodes are in the cluster, like so (this may be preferrable for larger clusters):

    voltdb start -B -H "voltserver1,voltserver2" -c 3

While the -H (host) parameter could list just a single server, that won't work in all cases for restarting a failed node, since the local process uses this hostname to find other nodes in the cluster, so at least two servers should generally be listed.


### Verifying that the database is available

You can tail the log file which is in a subdirectory "log" in the working folder where voltdb was started.  The database outputs "Server completed initialization." when it becomes available.

    tail -f log/volt.log

You can also tail the console output, which is in the "$HOME/.voltdb_server" directory, for example:

    tail -f $HOME/.voltdb_server/localhost_3021.out

Another way to check that the cluster initialized is to connect using sqlcmd.

    # from any host in the cluster
    sqlcmd
    > exit


## Loading the Schema

First compile any java stored procedures.  This example assumes the procedure java source files are in a subdirectory "./src/...", for example "./src/org/yourcompany/procedures/*.java"

    cd $HOME/voltdb_instance
    export CLASSPATH=$HOME/voltdb/voltdb/voltdb-*.jar
    SRC=`find src -name "*.java"`
    mkdir -p classes
    javac -classpath $CLASSPATH -d classes $SRC

Then package the class files into a .jar file (the classes are no longer needed at this point)

    jar cf procs.jar -C classes .
    rm -rf classes

Load the procedure classes into VoltDB

    sqlcmd
    1> load classes procs.jar;


Load the schema into VoltDB

    sqlcmd < ddl.sql


## Stopping the Cluster
From any server in the cluster, in any directory, run the following.

    # from any host in the cluster
    voltadmin shutdown --save


## Restarting the Cluster

On each of the servers (voltserver1, voltserver2, voltserver3), run the same command that was used to start the cluster before.

    # on each host
    voltdb start -B -H "voltserver1,voltserver2,voltserver3"

See the earlier section [Verifying that the database is available](Verifying-that-the-database-is-available) to check that the restart was successful and the data was reloaded.

Note: restarting should be used to bring up the database in exactly the same configuration as before.  To make configuration changes, use the maintenance window process described in the section [Stopping and restarting the cluster for a maintenance window](Stopping-and-restarting-the-cluster-for-a-maintenance-window) below.

## Pause and Resume the Cluster
To pause the cluster, from any server in the cluster, in any directory, run:

    # from any host in the cluster
    voltadmin pause

To resume the cluster, from any server in the cluster, in any directory, run:

    # from any host in the cluster
    voltadmin resume

## Updating the license file
When a license expires, the cluster will continue to run, but subsequent actions such as stopping and restarting may require that the new license be in place.  Prior to expiration, the new license should be updated.

    # from any host in the cluster
    voltadmin license <new_license_file.xml>

Alternatively, if the cluster has already been shut down, you can add the "--license <new_license_file.xml>" option to the "voltdb start" command you use to restart each host.

    # only needed once per host
    voltdb start --license <new_license_file.xml> ...

## Taking a manual backup
A snapshot is a point-in-time consistent copy of the entire contents of the database.  It is written to local disk at each node in a cluster to distribute the work of persistence.  A snapshot can be taken at any time, whether the database is online and available to users or in admin mode.

Note: The directory you take a snapshot into must exist on each server in the cluster, and the voltdb user must have write permission to it.

    # Take a snapshot with a given prefix
    voltadmin save /home/voltdb/voltdb_saved_snapshots my-backup

    # Or, use the current date and time as the "nonce" or prefix for the snapshot
    voltadmin save /home/voltdb/voltdb_saved_snapshots `date +"%Y%m%d-%H%M%S"`

Note: if you need to take a backup prior to stopping the cluster for maintenance, see the section below for additional steps.

## Restoring a manual backup
First, follow the steps documented above to start the cluster using the "voltdb init" and "voltdb start" commands and load the schema.  Then run the following command to load the snapshot data.

    # run from any directory
    voltadmin restore /home/voltdb/voltdb_saved_snapshots my-backup

Note: The files with this prefix need to be located in the specified path on one or more of the servers in the cluster.  For example, if you are loading a backup of production to test, you need to first copy these files from this path on the production servers to the same path on the test servers.


## Reconfiguring the Cluster during a Maintenance Window
To make major configuration changes to the database, such as changing the kfactor or sitesperhost settings, or modifying the configuration of command logging or export, the database must be stopped and restarted.  Using "voltdb start" to recover from the command log is for recovering the cluster in exactly the same configuration, not to make configuration changes, so the process involves reinitializing the database to the empty state using "voltdb init --force", restarting as if for the first time using "voltdb start", and then restoring a backup.  The additional commands "pause" and "resume" ensure that users do not try to add any data between when the snapshot (backup) is taken and when it is restored.


Pause the database (stop accepting requests from users)

    voltadmin pause

Take a manual snapshot

    voltadmin save --blocking /home/voltdb/voltdb_saved_snapshots snapshot_prefix

Note: the "--blocking" parameter means voltadmin will wait to make sure the snapshot was successful before returning.

Shut down the database

    voltadmin shutdown

Make changes to the configuration file and copy it to each of the servers.


Reinitialize the database root directory on all nodes, specifying the edited configuration file

    cd $HOME/voltdb_instance
    voltdb init --force --config=deployment.xml --license license.xml


Restart the database in admin mode

    # on each host
    cd $HOME/voltdb_instance
    voltdb start -B -H "voltserver1,voltserver2,voltserver3" --pause

(Optionally) Reload the schema, if any changes were made

    sqlcmd < ddl.sql

Reload the data from the snapshot

    voltadmin restore /home/voltdb/voltdb_saved_snapshots snapshot_prefix

Resume the database (allow users to connect and send requests)

    voltadmin resume


## Rejoining a failed node to the cluster
If a node failed, it can be restarted and joined back to the cluster using the same "voltdb start" command.

    # run from the app working folder where the deployment file is located
    cd /home/voltdb/voltdb_instance
    voltdb start -B -H "voltserver1,voltserver2,voltserver3"

To verify that the node has successfully rejoined the cluster, make sure you can connect to it with sqlcmd:

    sqlcmd

Or, you can check the volt.log file for "Node rejoin completed"

Note, if multiple nodes have failed, only one can be rejoined at a time. It is best to wait and verify that the first one has rejoined successfully before attempting to rejoin another.

If for some reason this node is not easily restarted (i.e. due to hardware failure) another node could be started and joined to the cluster to take the place of the failed node.  In this case, this new node would need to be prepared like all the other servers, with VoltDB and all the prerequisite software installed and NTP configured.

## Stopping a node manually
Some maintenance actions, especially to hardware or operating system can be performed on individual servers one at a time, without any downtime for the entire cluster.  In those cases, use the following command to stop an individual cluster, then see the previous section for how to rejoin a failed (or in this case a stopped) node.


    voltadmin stop voltserver1

Make sure that the previous node has successfully rejoined the cluster before proceeding to stop the next server. The voltadmin stop command will not allow you to stop a node if it would bring down the cluster, but it would allow multiple nodes to be shut down if the cluster would still be viable, but this would increase the risk of losing the cluster if anything were to happen to another node before they could all be rejoined.

## Adding nodes to increase capacity
To add more capacity (RAM as well as throughput) to the cluster, additional servers can be added to the cluster.  As with the others, these servers must be set up with VoltDB and all pre-requisite software installed and NTP configured.  The following command is used to have this node join the cluster.

    # run on each server to be added

    # the host parameter can be the hostname of any existing server in the cluster
    cd /home/voltdb/voltdb_instance
    voltdb init --license=license.xml
    voltdb start --host=voltserver1 --add

Note: K-factor + 1 is the number of servers that must be added at a time.  If the cluster is configured with k-factor=1, then 2 servers must be added.
