# Introduction

Testrun is a simple tool that builds on the run.sh files in example folders.
It helps make certain common types of repetitive manual testing easier. Some
aspects of it are geared toward the internal VoltDB testing environment. Other
aspects should be useful to almost any VoltDB user anywhere.

This has been tested by only one person in one environment. There will be
hiccups until it gets more mileage.

It is a semi-modular bash script. Any function with a name that begins with the
prefix "TEST_" becomes the name of a test that can be specified as the first
argument on the command line. For example, this function ...

    TEST_foo()
    {
        ...
    }

... can be run from the command line in an examples subfolder as follows.

    ../../tools/testrun foo

Any scripts assigned to the EXTSCRIPTS variable in testrun.cfg are sourced in
order to load additional TEST_... test commands the user would like to make
available locally.

It uses separate subfolders to isolate the data for each host in a cluster and
for replication hosts. This allows a shared NFS folder to server as a base for
testing across multiple machines in a cluster.

The script generates a customized deployment.xml based on data in testrun.cfg.
It does not use the local deployment.xml.

## Configuring a test environment.

* Change to an example directory.
* Run testrun with no arguments to generate testrun.cfg.
* Edit testrun.cfg to change settings.
* Run testrun with no arguments to see the available commands.
* Change EXTSCRIPTS if you want to add an extension script.
* Add additional tests to the extension script as TEST_... functions.

Here is a sample session:

    $ cd examples/voter
    $ testrun
    Generating "testrun.cfg" (edit before restarting)...
    $ vi testrun
    $ testrun
    Usage: testrun TEST [ ARG ... ]
       Where TEST is one of the following:
          clean
          client
          compile
          down
          dragent
          gateway
          recover
          recover_replica
          rejoin
          replica
          server
          shutdown
          status
          up
          update

## Working with multiple hosts.

Make sure your working directory is mounted on all hosts.

Server-related commands take a number argument for the host # that determines
which subfolder is used for data. This applies to the "server", "rejoin", and
"recover" commands, among others.

This example assumes the test environment is configured for 2 hosts.

    $ ssh HOST1
    $ cd examples/voter
    $ ../../tools/testrun server 1
    (voltdb host 1 starts)

    (from another shell...)

    $ ssh HOST2
    $ cd examples/voter
    $ ../../tools/testrun server 2
    (voltdb host 2 starts)

    (from another shell...)

    $ cd examples/voter
    $ ../../tools/testrun client
    (async benchmark runs)

The related server commands work the same, where the host number argument
determines where to find the existing database to work with.

### Testing replication.

Replication works similarly to the multiple host scenario. Additional
configuration variables in testrun.cfg provide information necessary for
replication, including the master host. Replication commands do not take a host
number argument since the current test assumes a single replication host.

The testrun script has the following commands for working with replication.
Network connectivity commands assume the Frankencluster environment. This is
also configurable in testrun.cfg.

* replica - Start the replica server.
* dragent - Start the dragent.
* down - Disconnect the replica subnet from the master subnet.
* up - Reconnect the replica subnet to the master subnet.
* gateway - Display the gateway server interfaces.
* recover_replica - Start the replica server in recovery mode.
