# Durability and Backups

## Command Logging for Durability

### Why Command Logging?

As an in-memory database, VoltDB performs the operations to insert, update, delete or retrieve data against data structures that are stored in main memory rather than on disk. The data is still written to disk, in a structure that is more optimal for high throughput rates. If the transactions had to wait for random disk I/O to retrieve and update a record stored somewhere in a file, then the database would lose the advantage of in-memory storage. Instead, information about the transactions can be appended to a log that can later be replayed to reconstruct the last state of all the records. If you have a way to save the entire state of the data at a given checkpoint, then you only need to keep a short-term log of recent transactions and recovery time can be decreased. This is how VoltDB preserves data on disk, using a feature called Command Logging, which is described in depth in the paper [Rethinking Main Memory OLTP Recovery](http://hstore.cs.brown.edu/papers/voltdb-recovery.pdf).

### What is the Command Log?

Command Logging is a light weight approach to disk persistence that records only the logical commands (procedure invocations) which will perform writes or DML statements such as insert, update, or delete SQL statements or the stored procedures that include them. The commands consist of the procedure or statement being called, the values of the input parameters, the timestamp when it was received, and the order in which it will be executed (transaction ID).

Notice that these are all things that are known before the transactions are executed. The previous state of the data doesn't need to be retrieved first, and the final result does not need to be determined in order to write these things to the log.  Therefore, the overhead of fine-grained logging of before and after images and the CPU and I/O involved can be avoided. The commands can be appended to files on disk either before or during the execution of the transaction, at a very high throughput rate. Whether it is played back from the beginning when the database was empty, or from some known checkpoint, as long as the the commands are replayed in the same order from a given starting point, the same commands and inputs will always produce the same end results, because SQL statements and stored procedures in VoltDB are required to be deterministic. Playing back the entire set of commands from the beginning would take a long time for most databases, so there needs to be a way to write a transactionally-consistent copy of all the data in the database so that you could reload it and only play back the subsequent commands in order to recover. For this, VoltDB uses a snapshot.


### What is a Snapshot?

A snapshot is a set of files that provide a transactional point-in-time consistent copy of all the records in a VoltDB database. For example, here is a snapshot taken on a single-node database with only one table:

    backup_001-HELLOWORLD-host_0.vpt
    backup_001-host_0.digest
    backup_001-host_0.finished
    backup_001-host_0.hash
    backup_001.jar

The files are prefixed with a name, in this case "backup_001". There are several overhead files, including a .jar that contains the schema and compiled procedure classes that the database was using at the time of the snapshot. There is a .finished file that is used to indicate to scripts that the snapshot is complete, so the files could be moved to another directory or uploaded to a backup system. Since my database only has one table, HELLOWORLD, there is a single .vpt file, which stores the records in binary format. This will include only records that were present on this server. If this had been a multi-node cluster, there would be a similar set of files in the same path on each server, each with a different "-host_..." suffix.

A snapshot can be taken while the database is running, even while it is running at extremely high rates of throughput, yet the snapshot contains a point-in-time consistent copy of the entire database. This is because when the snapshot is initialized the database uses a transaction to build a set of pointers to all the records in the tables at one point in time. This only takes a brief moment, then other transactions can proceed. If they insert any new records, they won't be seen by the snapshot. If they update any records, the original version will be left as-is but flagged as old, and the new version will be stored as a new tuple. If a record is deleted, it just gets marked. This way the snapshot can iterate through all of the records bit by bit in the background running in between the other transactions. When the snapshot is finished, it cleans any of the records that were marked as deleted or updated as they are no longer needed.

Generally taking a snapshot has a low impact on performance. When the database is not running at 100% throughput capacity, you might not notice any difference when a snapshot is running. If the database is working at maximum speed, you may see some low % dip in throughput.

Snapshots can be taken manually for backup purposes, but they are also taken automatically as needed as part of the command logging feature, and can optionally also be taken periodically by an automatic snapshot feature.

The command log will take it's own snapshots and write them to the "command_log_snapshot" folder. It retains only one complete snapshot. Once a new one is completed, the old one will be deleted. These are not taken "periodically" per se, but they are requested on demand whenever the command log reaches 1/2 of the logsize="..." configuration, which is 1GB by default. For example, if 500MB of command log have been written, it will request a snapshot and continue writing to the command log, which may sometimes grow beyond the logsize as it is not a limit but just a target size that determines the timing of snapshots. Once the snapshot is complete, the command in the log prior to that point in time are truncated, keeping the command log size and the corresponding recovery time within manageable limits.


### Configuring Command Log

Command logging is configured in the deployment file and set at startup. It has a few configurable options:
 - mode (synchronous or asynchronous)
 - frequency (how often to write to disk)
 - log size (how large logs can grow before a snapshot is requested)
 - path for command log files
 - path for command log snapshots

*Using VoltDB* 14.3: [Configuring Command Logging for Optimal Performance](https://docs.voltdb.com/UsingVoltDB/CmdLogConfig.php) has an excellent step-by-step description of each of these options. Here is a typical configuration that uses asynchronous mode and is well suited for development and production deployments on servers that have only a single disk:

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

This allows the frequency for fsync to disk to be set to 1-2 milliseconds or a smaller number of transactions, because the disk is either an SSD or has a battery-backed write cache, and it is dedicated mainly to the command log so writes won't be impeded by snapshots or other disk I/O such as log4j files or export files. The logsize is set to 5GB rather than the default 1GB, which reduces the frequency of command log snapshots, but for high write throughput workloads results in a fairly typical frequency rather than running continuously. See [Using VoltDB: Configuring Command Logging for Optimal Performance](https://docs.voltdb.com/UsingVoltDB/CmdLogConfig.php) for more detail.

## Manual and Scheduled Backups

If you have command logging enabled, then that is sufficient for durability to disk. However, you may need to take backups as part of maintenance or to have the option to restore data that was deleted by mistake or just for additional peace of mind.

The "voltadmin save" command is a manual way to request a snapshot.  Just like a backup, it is used frequently as part of a maintenance process, so you can revert back if you make a mistake. It can also be useful to migrate data from one cluster to a new set of hardware. For example, if you just want to take a backup without interfering with users or other transaction activity, you would use the "voltadmin save" command like this:

    voltadmin  save /path/to/backups name_of_backup

Some operators will invoke this command from an automated script or cron job to request snapshots at prescribed times.

VoltDB also has an automatic snapshot feature which you can enable to take periodic snapshots. This can be enabled in the deployment.xml file, and has a few configurable options (see [Using VoltDB: Scheduling Automated Snapshots](https://docs.voltdb.com/UsingVoltDB/SaveSnapshotAuto.php) for more details):
 - frequency: (how often to take a snapshot) - you can specify whole units of time using the suffixes "s", "m", or "h" for seconds, minutes, or hours. The default is 24 hours
 - prefix: this is the prefix used for the snapshot files (default is "AUTOSNAP")
 - retain: the number of snapshots to retain. Older snapshots are purged once this limit is reached.

For example, the following configuration would take a snapshot every 30 minutes and retain the most recent three snapshots:

    <?xml version="1.0"?>
    <deployment>
        <cluster sitesperhost="8"/>
        <snapshot prefix="AUTOSNAP" frequency="30m" retain="3"/>
        <httpd enabled="true">
            <jsonapi enabled="true" />
        </httpd>
    </deployment>

Suppose you want to take daily or hourly backups, and you're considering automatic snapshots vs. calling "voltadmin save" from a cron job.  Both are perfectly valid, but here are some considerations:

* Automated snapshots can just be configured in the deployment.xml file, no separate automation or configuration is required
* Automated snapshots will handle retention of N copies automatically, not extra scripting needed to delete older snapshots
* Manual snapshots can be scheduled with cron to run exactly on the hour or at a specified time. Automatic snapshots run periodically, but the time cannot be specified, it is based on when the database was started.
* Automatic snapshots are written to the snapshots folder, which is typically a subfolder of voltdbroot. You can override the path. Manual snapshotsare written to whatever folder is specified each time.
* Automatic snapshots have a long name based on the timestamp. For manual snapshots, you specify the name, but it must be unique within the given folder.
* Manual snapshots can be output in binary or CSV format. Automatic snapshots are only in binary format.

### Archiving backups

If you want to copy or move snapshot files somewhere for backup purposes, you should first check if there is a ".finished" file which is created after the snapshot files are complete. This prevents you from moving or interfering with files that are still being written.

When moving or copying large files over the network, it is always a good idea to use a bandwidth limit so that the file transfer does not interfere with the normal flow of messages between VoltDB servers in a cluster. For example, you might use "scp -l ..." or "rsync --bwlimit ...".
