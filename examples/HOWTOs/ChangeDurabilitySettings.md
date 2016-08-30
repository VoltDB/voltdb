# Change Durability Settings

VoltDB supports the following persistence/durability options:

- No disk persistence.
- Periodic cluster-wide snapshots.
- Asynchronous command-log with periodic truncation snapshots.
- Synchronous command-log with periodic truncation snapshots.

How to Use a Deployment File
-----------------------------------------

In order to change the default persistence settings, you're going to need to initialize the server with a deployment file.

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
voltdb start
```

Command Logging
-----------------------------------------

Command logging persists a log of logical commands in execution order for each partition to disk. This allows VoltDB to recover data between snapshots in the event of a cluster-wide failure.

Command logging is configured in the deployment file and set at startup. It supports asynchronous and synchronous modes with various configuration options. In the commercial edition of VoltDB, the default setting is asynchronous command logging enabled.

See `deployment-noPersistence-PureMemory`, `deployment-asyncPersistence.xml`, and `deployment-syncPersistence.xml` in `deployment-file-examples` for example deployment files.

See the chapter 14 in the Using VoltDB book for more information on configuring command logging.

Using VoltDB Chapter 14: Command Logging and Recovery
https://docs.voltdb.com/UsingVoltDB/ChapCmdLog.php
Section 14.3: Configuring Command Logging for Optimal Performance
https://docs.voltdb.com/UsingVoltDB/CmdLogConfig.php

Periodic Snapshots
-----------------------------------------

VoltDB can be configured to save periodic cluster-wide, transactional snapshots by adding configuration to the deployment file.

Note if you are using command logging, these snapshots are in addition to periodic truncation snapshots.

See `deployment-scheduledPeriodicSnapshots.xml` in `deployment-file-examples` for an example deployment file.

Using VoltDB Section 13.2: Scheduling Automated Snapshots
https://docs.voltdb.com/UsingVoltDB/SaveSnapshotAuto.php

Paths
-----------------------------------------

By default, all user data written to disk is written to the `voltdbroot` folder, which lives, by default, in the directory where the server process was started.

It is possible to specify where the `voltdbroot` folder is placed using the `--dir or -D` init and start command line option.

You can also change the location of the command log, the command log truncation snapshots, any periodic snapshots, and the export overflow data by specifying their respective paths elements in the deployment file provided at `voltdb init` command.

You can find examples of these changes in section 14.3.4 of the Using VoltDB book.

One reason you might want to do this is to use dedicated disks for some of these high-throughput write directories. If disk performance is limiting, separating truncation snapshots and command log data onto different disks may help. It also might help with storage needs, if snapshots are particularly large, for example.

Using VoltDB Section 14.3.4:
Configuring Command Logging for Optimal Performance
Hardware Considerations
https://docs.voltdb.com/UsingVoltDB/CmdLogConfig.php

Using VoltDB Appendix E.2.: The Structure of the Deployment File
https://docs.voltdb.com/UsingVoltDB/ConfigStructure.php
