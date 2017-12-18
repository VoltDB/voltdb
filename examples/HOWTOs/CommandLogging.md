# Command Logging

VoltDB uses Command Logging to continuously save to disk the latest state of the data maintained in main memory. Rather than updating data on disk in a logical structure similar to how it is stored in memory, it relies on snapshots to provide a given checkpoint and stores the commands that modified the state from that checkpoint in an append-only log. This improves performance by using continuous writes to disk rather than random disk I/O, so that the disk persistence is able to keep up with the high rate of transaction processing that VoltDB achieves in memory. Snapshots are taken automatically as needed to keep disk usage and recovery from excessively growing.

### Configuring Command Log

Command logging is configured in the deployment file and set at startup. It has a few configurable options:
 - mode (synchronous or asynchronous)
 - frequency (fsync to disk when given time in milliseconds or number of transactions have elapsed, whichever comes first)
 - log size (size in MB the command log should grow before a snapshot is taken)
 - path for command log files
 - path for command log snapshots

Here is a typical configuration that uses asynchronous mode and is well suited for development and production deployments on servers that have only a single disk:

    <?xml version="1.0"?>
    <deployment>
        <cluster sitesperhost="8"/>
        <commandlog enabled="true" synchronous="false">
            <frequency time="200" transactions="100000"/>
        </commandlog>
        <httpd enabled="true">
            <jsonapi enabled="true" />
        </httpd>
    </deployment>

Here is a more advanced command log configuration using synchronous mode with a separate fast disk dedicated for the command log:

    <?xml version="1.0"?>
    <deployment>
        <cluster sitesperhost="8"/>
        <paths>
            <commandlog path="/fastdisk/voltdb/command_log/" />
            <commandlogsnapshot path="/maindisk/voltdb/command_log_snapshot/" />
        </paths>
        <commandlog enabled="true" synchronous="true" logsize="5120">
            <frequency time="2" transactions="500"/>
        </commandlog>
        <httpd enabled="true">
            <jsonapi enabled="true" />
        </httpd>
    </deployment>

This allows the frequency for fsync to disk to be set to 1-2 milliseconds or a smaller number of transactions, because the disk is either an SSD or has a battery-backed write cache, and it is dedicated mainly to the command log so writes won't be impeded by snapshots or other disk I/O such as log4j files or export files. The log size is set to 5GB rather than the default 1GB, which reduces the frequency of command log snapshots, but for high write throughput workloads results in a fairly typical frequency rather than running continuously.

Reference:

  [Using VoltDB: Command Logging and Recovery](https://docs.voltdb.com/UsingVoltDB/ChapCmdLog.php) - Describes in more detail how command logging works.

  [Using VoltDB: Configuring Command Logging for Optimal Performance](https://docs.voltdb.com/UsingVoltDB/CmdLogConfig.php) - Contains an excellent step-by-step description of the configurable options of command logging and the hardware considerations.

  [Rethinking Main Memory OLTP Recovery](http://hstore.cs.brown.edu/papers/voltdb-recovery.pdf) - An academic computer science paper that describes the reasoning behind command logging.
