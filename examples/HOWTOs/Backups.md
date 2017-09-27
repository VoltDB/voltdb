# Backups

A VoltDB database can be saved to disk for backup purposes using a feature called snapshot. A snapshot is a set of files that provide a transactional point-in-time consistent copy of all the records in a VoltDB database. Snapshots can be taken manually for backup purposes, periodically using the automated snapshots feature, and when needed by the command logging feature.

## Manual Backups

If you have command logging enabled, then that is sufficient for durability to disk. However, you may need to take a backup before performing maintenance, to protect against user mistakes, or simply for peace of mind.

The "voltadmin save" command is a manual way to request a snapshot.  Like any backup, it is used frequently as part of a maintenance process so you can revert back to some checkpoint if a mistake is made or the maintenance fails to achieve the desired results. It can also be useful to migrate data at one time from one cluster to another. For example, to take a backup without interfering with users or other transaction activity, you would use the "voltadmin save" command like this:

    voltadmin  save /path/to/backups name_of_backup

This command can be automated using a cron job or similar script to take a manual snapshot periodically. The advantage of this is that the snapshot will be taken at a prescribed time such as every hour on the hour or once a day at midnight. You can also specify "--format=csv" for the snapshot data to be saved as CSV files, which can be useful for periodically loading the contents of entire VoltDB tables to some other system.

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


The advantage of using automated snapshots is that it only needs to be configured in the deployment.xml file and runs entirely within the database, so no separate automation or configuration is required. It also retains a given number of snapshots and deletes older snapshots automatically.

### Archiving Backups

If you want to copy or move snapshot files somewhere for backup purposes, you should first check if there is a ".finished" file which is created after all the snapshot files are complete. This prevents you from moving or interfering with files that are still being written. For example, here are the files included in a very simple snapshot with the name "backup_001" taken from a single-node database consisting of only one "HELLOWORLD" table:

    backup_001-HELLOWORLD-host_0.vpt
    backup_001-host_0.digest
    backup_001-host_0.finished
    backup_001-host_0.hash
    backup_001.jar

When moving or copying large files over the network, it is always a good idea to use a bandwidth limit so that the file transfer does not interfere with the normal flow of messages between VoltDB servers in a cluster. For example, you might use "scp -l ..." or "rsync --bwlimit ...".
