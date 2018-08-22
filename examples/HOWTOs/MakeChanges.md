# Making Changes to a VoltDB cluster

Here is information and links to help you plan for changes on a live system.

## Changing the Schema

Most schema changes can be made online using CREATE or ALTER commands via SQLCMD.  *Using VoltDB* has an overview of this, [Modifying the Schema](https://docs.voltdb.com/UsingVoltDB/SchemaModify.php).

There are a few types of schema changes that require taking the database offline following the process [Performing Update Using Save and Restore](https://docs.voltdb.com/AdminGuide/Maintainschema.php#MaintainSchemaSaveRestore) from *Administrator's Guide*. This is required only for the following types of schema changes:

 - Changing the partitioning of a table that is not empty
 - Adding a unique index to a table that is not empty

### Changing Stored Procedures

You can load, replace or delete stored procedure Java code using the "load/remove classes" directives in sqlcmd. For more information, see the  the "Class management directives" section in the [sqlcmd reference page](https://docs.voltdb.com/UsingVoltDB/clisqlcmd.php) in *Using VoltDB*.

## Changing the Deployment Configuration

You can change the configuration of the following settings by editing the deployment.xml file and uploading it using the following command:

    voltadmin update deployment.xml

If you do not have a copy of the latest deployment.xml file that is used by your database, you can retrieve it using the following command:

    voltdb get deployment

Allowable changes include the following (see the ['update' section of the voltadmin reference page](https://docs.voltdb.com/UsingVoltDB/clivoltadmin.php) in *Using VoltDB*):

 - Security settings, including user accounts
 - Import and export settings (including add or remove configurations, or toggle enabled=true/false)
 - Database replication settings (except the DR cluster ID)
 - Automated snapshots
 - System settings:
    - Heartbeat timeout
    - Query Timeout
    - Resource Limit — Disk Limit
    - Resource Limit — Memory Limit

Otherwise, you need to apply the change using a maintenance window process which involves taking a snapshot and then using the `voltdb init` command to initialize a new database instance initialized with the modified deployment.xml file, after which you can restart and restore the snapshot. See [Reconfiguring the Cluster During a Maintenance Window](https://docs.voltdb.com/AdminGuide/MaintainUpgradeHw.php#MaintainUpgradeRestart) in *Administrator's Guide*. Specifically, you need to use this process to change:

 - temp table limit
 - paths
 - ports
 - command logging configuration
 - any cluster attributes (e.g. K safety or sites per host).
 - enable/disable HTTP interface
