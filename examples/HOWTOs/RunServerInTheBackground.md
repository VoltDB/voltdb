# Running VoltDB Server as a Daemon (in the background)

Relevant Documentation
-----------------------------------------

The VoltDB CLI documentation can be found here:
https://docs.voltdb.com/UsingVoltDB/clivoltdb.php
See the section on *Database Startup Arguments*.

Before backgrounding VoltDB, you might want to read more about shutdown as well:
https://docs.voltdb.com/UsingVoltDB/RunStartDBStop.php


On the Command Line
-----------------------------------------

The simplest way to start VoltDB is by using a command like this:

```bash
voltdb init
voltdb start
```

But you may typically be starting VoltDB with options, like:

```bash
voltdb init --force -C deployment.xml
voltdb start -c [cluster-node-count] -H coordinator1,coordinator2
```

Both of these commands will start VoltDB in the foreground of a Linux or OS X console. By default, log messages sent to the CONSOLE log4j appender will go to the console's standard output, while more detailed logging will go to the `voltdb.log` in the `voltdbroot/log` folder on the filesystem.

To start as a daemon, or backgrounded process, we use the `-B` or `--background` option, like so:

```bash
voltdb start -B -c [cluster-node-count] -H coordinator1,coordinator2
```

This will allow you to close the terminal or shell and the process will continue running. It will also allow you to run other commands. You will notice that log messages still go to standard output, possibly interfering with any later commands you try to run on this shell. To redirect the console log output to a file named `nohup.out`, add to the end of the command like so:

```bash
voltdb start -B -c [cluster-node-count] -H coordinator1,coordinator2 > nohup.log 2>&1 &
```

Now that the process is backgrounded, you can no longer kill the server process with ctrl-c. You have two options for stopping.

First, to shut down a whole cluster, use the `voltdbadmin` command like so:

```bash
voltadmin shutdown
```

To stop just one VoltDB process, use the `voltadmin` command like so:

```bash
voltadmin stop TARGET_HOST
```
where TARGET_HOST identifies the process to be stopped, by hostname or address, and optionally a port number.

For furher information, see the `shutdown` documentation https://docs.voltdb.com/UsingVoltDB/RunStartDBStop.php, and the `stop` documentation https://docs.voltdb.com/AdminGuide/OpsNodes.php, on the VoltDB web site.

Bash Scripting and Editing the run.sh Script
-----------------------------------------

Some previous versions of the example run.sh script had targets for starting VoltDB in the background, and even waiting for it to start. These targets were removed in the interest of simplicity, but they are preserved here.

If you would like to add these back to your run.sh, or to your own VoltDB Bash scripts, go right ahead. If you're using these in your own scripts, note these require the VoltDB `bin` directory to be in your path.

```bash
# wait for backgrounded server to start up
function wait_for_startup() {
    # use sqlcmd
    until echo "exec @SystemInformation, OVERVIEW;" | sqlcmd > /dev/null 2>&1
    do
        sleep 2
        echo " ... Waiting for VoltDB to start"
        if [[ $SECONDS -gt 60 ]]
        then
            echo "Exiting.  VoltDB did not startup within 60 seconds" 1>&2;
            exit 1;
        fi
    done
}
```


```bash
# startup server in background and load schema
function background_server_andload() {
    # run the server in the background
    voltdb init --force -C deployment.xml
    voltdb start -B -c $CLUSTERHOSTCOUNT \
        -H $COORDINATORHOSTS > nohup.log 2>&1 &
    # block until the server is accepting work
    wait_for_startup
    # load schema, procedures and possibly data
    init
}
```
