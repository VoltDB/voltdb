What is Volt Active Data?
====================

Thank you for your interest in Volt Active Data!

Volt Active Data provides reliable data services for today's demanding applications. A distributed, horizontally-scalable, ACID-compliant database that provides streaming, storage, and real-time analytics for applications that benefit from strong consistency, high throughput and low, predictable latency.

Volt Active Data and Open Source
====================

2023: Volt Active Data’s Open Source version is now frozen
--------------------

At the end of 2022 we stopped updating this Github repo. While this code exists, and can be used, it is not an ‘official’ version of Volt. It does not receive updates. We will not provide any form of support for it, and if you end up buying Volt you will need to move to the official version. 

Evaluating Volt using this version
--------------------

This version is provided for students, hobbyists, and researchers interested in testing  an ultra-fast, scalable ACID database. But all new and production-ready features are available through the commercial product from https://www.voltactivedata.com.

If you are looking at using Volt in a real-world business context, then we would strongly recommend that you speak to us directly. We have lots of experience helping people evaluate data platforms, and can set you up with demonstration clusters on AWS running representative workloads. We know from experience that this drastically cuts down the time needed to get to a ‘Go/No Go’ decision. Bear in mind that a lot of our customers are doing complicated things like cross center data replication, kubernetes or very high transactions per second, and we can help you see these features in action without you having to build a deployment from scratch. 

Different Versions
--------------------

Volt Active Data offers the fully open source, AGPL3-licensed Community Edition of Volt Active Data through GitHub here:

https://github.com/voltdb/voltdb/

Trials of the enterprise edition of Volt Active Data are available from the Volt Active Data website at the following URL:

https://www.voltactivedata.com

The Community Edition has full application compatibility and provides everything needed to run a real-time, in-memory SQL database with datacenter-local redundancy and snapshot-based disk persistence.

The commercial editions add operational features to support industrial-strength durability, manageability, and availability, including per-transaction disk-based persistence, multi-datacenter replication, elastic online expansion, live online upgrade, etc.

For more information, please visit the Volt Active Data website.

https://voltactivedata.com

VoltDB Branches and Tags
====================

The latest development branch is _master_. We develop features on branches and merge to _master_ when stable. While _master_ is usually stable, it should not be considered production-ready and may also have partially implemented features.

Code that corresponds to released versions of Volt Active Data are tagged "voltdb-X.X" or "voltdb-X.X.X". To build corresponding OSS VoltDB versions, use these tags.


Building Volt Active Data
====================

Information on building Volt Active Data from this source repository is maintained in a GitHub wiki page available here:

https://github.com/VoltDB/voltdb/wiki/Building-VoltDB


First Steps
====================

From the directory where you installed Volt Active Data, you can either use bin/{command} or add the bin folder to your path so you can use the Volt Active Data commands anywhere. For example:

    PATH="$PATH:$(pwd)/bin/"
    voltdb --version
    
Then, initialize a root directory and start a single-server database. By default the root directory is created in your current working directory. Or you can use the --dir option to specify a location:

    voltdb init [--dir ~/mydb]
    voltdb start [--dir ~/mydb] [--background]
    
To start a SQL console to enter SQL DDL, DML or DQL:

    sqlcmd
    
To launch the web-based Volt Management Console (VMC), open a web browser and connect to localhost on port 8080 (unless there is a port conflict): http://localhost:8080.
    
To stop the running Volt Active Data cluster, use the shutdown command. For commercial customers, the database contents are saved automatically by default. For open-source users, add the --save argument to manually save the contents of your database:

    voltadmin shutdown [--save]
    
Then you can simply use the start command to restart the database:

    voltdb start [--dir ~/mydb] [--background]
    
Further guidance can be found in the tutorial: https://docs.voltactivedata.com/tutorial/. For more on the CLI, see the documentation: https://docs.voltactivedata.com/UsingVoltDB/clivoltdb.php.


Next Steps
====================

### Examples

You can find application examples in the "examples" directory inside this Volt Active Data kit. The Voter app ("examples/voter") is a great example to start with. See the README to learn what it does and how to get it running.

The "examples" directory provides additional examples and a README explaining how to run them.

### Tutorial

The Volt Active Data Tutorial walks you through building and running your first Volt Active Data application.

https://docs.voltactivedata.com/tutorial/

### Documentation

The _Using VoltDB_ guide and supporting documentation is comprehensive and easy to use. It's a great place for broad understanding or to look up something specific.

https://docs.voltactivedata.com/

### Go Full Cloud

For information on using VoltDB in the Cloud, see the _Volt Kubernetes Administrator's Guide_.

https://docs.voltactivedata.com/KubernetesAdmin/


What's Included
====================

If you have installed Volt Active Data from the distribution kit, you now have a directory containing this README file and several subdirectories, including:

- **bin** - Scripts for starting Volt Active Data, bulk loading data, as well as interacting with and managing the running database. Including:
  - bin/voltdb - Start a Volt Active Data process.
  - bin/voltadmin - CLI to manage a running cluster.
  - bin/sqlcmd - SQL console. 
- **doc** - Documentation, tutorials, and java-doc
- **examples** - Sample programs demonstrating the use of Volt Active Data
- **lib** - Third party libraries
- **tools** - XML schemas, monitoring plugins, and other tools
- **voltdb** - the Volt Active Data binary software itself including:
  - log4j files - Logging configuration.
  - voltdbclient-version.jar - Java/JVM client for connecting to VoltDB, including native Volt Active Data client and JDBC driver.
  - voltdb-version.jar - The full Volt Active Data binary, including platform-specific native libraries embedded within the jar. This is a superset of the client code and can be used as a native client driver or JDBC driver.


Commercial Volt Active Data Differences
====================

Volt Active Data offers sandboxes and a pre-built trial version of Volt Active Data for application developers who want to try out the product. See the Volt Active Data website for more information.

https://voltactivedata.com/


Getting Help & Providing Feedback
====================

If you have any questions or comments about Volt Active Data, we encourage you to reach out to the Volt Active Data team and community through stack overflow:

https://stackoverflow.com/questions/tagged/voltdb.



Licensing
====================

This program is free software distributed under the terms of the GNU Affero General Public License Version 3. See the accompanying LICENSE file for details on your rights and responsibilities with regards to the use and redistribution of Volt Active Data software.
