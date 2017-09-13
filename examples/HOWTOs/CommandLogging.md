# Command Logging

### Why Command Logging?

As an in-memory database, VoltDB performs the operations to insert, update, delete or retrieve data against data structures that are stored in main memory rather than on disk. The data is still written to disk, in a structure that is more optimal for high throughput rates. If the transactions had to wait for random disk I/O to retrieve and update a record stored somewhere in a file, then the database would lose the advantage of in-memory storage. Instead, information about the transactions can be appended to a log that can later be replayed to reconstruct the last state of all the records. If you have a way to save the entire state of the data at a given checkpoint, then you only need to keep a short-term log of recent transactions and recovery time can be decreased. This is how VoltDB preserves data on disk, using a feature called Command Logging, which is described in depth in the paper [Rethinking Main Memory OLTP Recovery](http://hstore.cs.brown.edu/papers/voltdb-recovery.pdf).

### What is the Command Log?

Command Logging is a light weight approach to disk persistence that records only the logical commands (procedure invocations) which will perform writes or DML statements such as insert, update, or delete SQL statements or the stored procedures that include them. The commands consist of the procedure or statement being called, the values of the input parameters, the timestamp when it was received, and the order in which it will be executed (transaction ID).

Notice that these are all things that are known before the transactions are executed. The previous state of the data doesn't need to be retrieved first, and the final result does not need to be determined in order to write these things to the log.  Therefore, the overhead of fine-grained logging of before and after images and the CPU and I/O involved can be avoided. The commands can be appended to files on disk either before or during the execution of the transaction, at a very high throughput rate. Whether it is played back from the beginning when the database was empty, or from some known checkpoint, as long as the commands are replayed in the same order from a given starting point, the same commands and inputs will always produce the same end results, because SQL statements and stored procedures in VoltDB are required to be deterministic. Playing back the entire set of commands from the beginning would take a long time for most databases, so there needs to be a way to write a transactionally-consistent copy of all the data in the database so that you could reload it and only play back the subsequent commands in order to recover. For this, VoltDB uses a snapshot. Read [Backups](Backups.md) for more on how snapshots work.

While snapshots can be taken manually or periodically using other features, the command log also takes its own snapshots in the "command_log_snapshot" folder whenever needed. Only one complete snapshot is retained. Once a new one is completed, the old one will be deleted. These are not taken "periodically" per se, but they are requested on demand whenever the command log reaches 1/2 of the logsize="..." configuration, which is 1GB by default. For example, if 500MB of command log has been written, it will request a snapshot and continue writing to the command log, which may sometimes grow beyond the "logsize" setting as it is not a limit but just a target size that determines the timing of snapshots. Once the snapshot is complete, all of the commands prior to that checkpoint in the log are truncated. This keeps the command log size and the corresponding recovery time within manageable limits.

### Configuring Command Log

Command logging is configured in the deployment file and set at startup. It has a few configurable options:
 - mode (synchronous or asynchronous)
 - frequency (how often to write to disk)
 - log size (how large logs can grow before a snapshot is requested)
 - path for command log files
 - path for command log snapshots

Here is a typical configuration that uses asynchronous mode and is well suited for development and production deployments on servers that have only a single disk:

    <?xml version="1.0"?>
    <deployment>
        <cluster sitesperhost="8"/>
        <commandlog enabled="true" synchronous="false">
            <frequency time="200" transactions="500"/>
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
            <frequency time="2" transactions="100"/>
        </commandlog>
        <httpd enabled="true">
            <jsonapi enabled="true" />
        </httpd>
    </deployment>

This allows the frequency for fsync to disk to be set to 1-2 milliseconds or a smaller number of transactions, because the disk is either an SSD or has a battery-backed write cache, and it is dedicated mainly to the command log so writes won't be impeded by snapshots or other disk I/O such as log4j files or export files. The log size is set to 5GB rather than the default 1GB, which reduces the frequency of command log snapshots, but for high write throughput workloads results in a fairly typical frequency rather than running continuously.

Reference:

  See [Using VoltDB: Configuring Command Logging for Optimal Performance](https://docs.voltdb.com/UsingVoltDB/CmdLogConfig.php) for an excellent step-by-step description of the configurable options of command logging and the hardware considerations.
