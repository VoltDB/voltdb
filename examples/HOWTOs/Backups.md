# Backups

A VoltDB database can be backed up to disk using a feature called snapshot.

## What is a Snapshot?

A snapshot is a set of files that provide a transactional point-in-time consistent copy of all the records in a VoltDB database. For example, here is a snapshot that was taken on a single-node database with only one table:

    backup_001-HELLOWORLD-host_0.vpt
    backup_001-host_0.digest
    backup_001-host_0.finished
    backup_001-host_0.hash
    backup_001.jar

The files are prefixed with a name, in this case, "backup_001". There are several overhead files, including a .jar that contains the schema and compiled procedure classes that the database was using at the time of the snapshot. There is a .finished file that is used to indicate to scripts that the snapshot is complete, so the files could be moved to another directory or uploaded to a backup system. Since my database only has one table, HELLOWORLD, there is a single .vpt file, which stores the records in binary format. This will include only records that were present on this server. If this had been a multi-node cluster, there would be a similar set of files in the same path on each server, each with a different "-host_..." suffix.

A snapshot can be taken while the database is running, even while it is running at extremely high rates of throughput, yet the snapshot contains a point-in-time consistent copy of the entire database. This is because when the snapshot is initialized the database uses a transaction to build a set of pointers to all the records in the tables at one point in time. This only takes a brief moment, then other transactions can proceed. If they insert any new records, they won't be seen by the snapshot. If they update any records, the original version will be left as-is but flagged as old, and the new version will be stored as a new tuple. If a record is deleted, it just gets marked. This way the snapshot can iterate through all of the records bit by bit in the background running in between the other transactions. When the snapshot is finished, it cleans any of the records that were marked as deleted or updated as they are no longer needed.

Generally taking a snapshot has a low impact on performance. When the database is not running at 100% throughput capacity, you might not notice any difference when a snapshot is running. If the database is working at maximum speed, you may see some low % dip in throughput.

Snapshots can be taken manually for backup purposes, periodically using the automated snapshots feature, and they are also taken as often as needed as part of the command logging feature.

## Manual Backups

If you have command logging enabled, then that is sufficient for durability to disk. However, you may need to take backups as part of maintenance or to have the option to restore data that was deleted by mistake or just for additional peace of mind.

The "voltadmin save" command is a manual way to request a snapshot.  Just like a backup, it is used frequently as part of a maintenance process, so you can revert back if you make a mistake. It can also be useful to migrate data from one cluster to a new set of hardware. For example, if you just want to take a backup without interfering with users or other transaction activity, you would use the "voltadmin save" command like this:

    voltadmin  save /path/to/backups name_of_backup

Some operators will invoke this command from an automated script or cron job to request snapshots periodically. The main advantage is that the snapshot will be taken at a prescribed time such as every hour on the hour or once a day at midnight. The other advantage is if you want the snapshot in CSV format you can specify "--format=csv".

## Periodic Backups

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


The advantage of this feature is that it is more automated than scripted manual backups. It is configured in the deployment.xml file and runs entirely within the database, so no separate automation or configuration is required. It automatically retains a given number of snapshots, you don't need to write scripts to delete older snapshots.

### Archiving Backups

If you want to copy or move snapshot files somewhere for backup purposes, you should first check if there is a ".finished" file which is created after the snapshot files are complete. This prevents you from moving or interfering with files that are still being written.

When moving or copying large files over the network, it is always a good idea to use a bandwidth limit so that the file transfer does not interfere with the normal flow of messages between VoltDB servers in a cluster. For example, you might use "scp -l ..." or "rsync --bwlimit ...".
