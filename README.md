What is VoltDB?
====================

Thank you for your interest in VoltDB!

VoltDB is a horizontally-scalable, in-memory SQL RDBMS designed for applications that have extremely high read and write throughput requirements.


Building VoltDB
====================

Information on building VoltDB from this source repository is maintained in a GitHub wiki page available here:

https://github.com/VoltDB/voltdb/wiki/Building-VoltDB


First Steps
====================

From the current directory, to start a single-server VoltDB database.

    bin/voltdb create [--background]
    
To start a SQL console to enter SQL DDL, DML or DQL:

    bin/sqlcmd
    
To launch the web-based VoltDB Management Console (VMC), open a web browser and connect to localhost on port 8080 (unless there is a port conflict): http://localhost:8080.
    
To stop the running VoltDB cluster, use CTRL-C for foreground VoltDB, and use the VoltDB Admin CLI for backgrounded or multi-node clusters:

    bin/voltadmin shutdown
    
You may also want to optionally add the bin directory to your PATH environment variable. This will allow you to use VoltDB and its tools from any directory.
    
Further guidance can be found in the tutorial: https://docs.voltdb.com/tutorial/. For more on the CLI, see the documentation: https://docs.voltdb.com/UsingVoltDB/clivoltdb.php.


Next Steps
====================

### Examples

You can find application examples in the “examples” directory inside this VoltDB kit.

The Voter app (“examples/voter”) is a great example to start with. See the README to learn what it does and how to get it running, or watch this 5 minute video demonstration of the Voter app:
https://voltdb.com/resources/video/voltdb-how-tour-voter-example

The App Gallery has more information on additional examples, some in the kit and some on GitHub.

- App Gallery: https://voltdb.com/community/applications
- Supplemental Examples: https://github.com/VoltDB/?query=app-

### Tutorial

The VoltDB Tutorial will walk you through building and running your first VoltDB application.

https://docs.voltdb.com/tutorial/

### Documentation

The VoltDB User Guide and supporting documentation is comprehensive and easy to use. It’s a great place for broad understanding or to look up something specific.

https://docs.voltdb.com

### Product Overview

The VoltDB Product page contains info at a higher level. This page has in-depth descriptions of features that explain not just what, but why. It also covers use cases and competitive comparisons.

https://voltdb.com/product

### Go Full Cloud

For information on using VoltDB virtualized, containerized or in the Cloud, see the the VoltDB website.

https://voltdb.com/run-voltdb-virtualized-containerized-or-cloud


What's Included
====================

If you have installed VoltDB from the distribution kit, you now have a directory containing this README file and several subdirectories, including:

- **bin** - Scripts for starting VoltDB, bulk loading data, as well as interacting with and managing the running database. Including:
  - bin/voltdb - Start a VoltDB process.
  - bin/voltadmin - CLI to manage a running cluster.
  - bin/sqlcmd - SQL console. 
- **doc** - Documentation, tutorials, and java-doc
- **examples** - Sample programs demonstrating the use of VoltDB
- **lib** - Third party libraries
- **tools** - XML schemas, monitoring plugins, and other tools
- **voltdb** - the VoltDB binary software itself including:
  - license.xml - An embedded trial license.
  - log4j files - Logging configuration.
  - voltdbclient-version.jar - Java/JVM client for connecting to VoltDB, including native VoltDB client and JDBC driver.
  - voltdb-version.jar - The full VoltDB binary, including platform-specific native libraries embedded within the jar. This is a superset of the client code and can be used as a native client driver or JDBC driver.


Commercial VoltDB Differences
====================

VoltDB offers a pre-built binary distribution of VoltDB under a commercial license. It can be downloaded from the VoltDB website. This download includes a 30 day trial license.

https://voltdb.com/download

When to use this open-source version:

- When developing applications (as long as they don't need VoltDB Export).
- When performance testing in non-redundant configurations.
- When reading or modifying source code.

When to use the commercial version:

- When disk persistence is required.
- When the VoltDB Export feature is required.
- When high availability features like redundant clustering, live node rejoin, and multi-datacenter replication are required.


Getting Help & Providing Feedback
====================

If you have any questions or comments about VoltDB, we encourage you to reach out to the VoltDB team and community.

- **VoltDB Forums** - Create threads, post responses and search existing posts on our community forums at https://forum.voltdb.com.
- **VoltDB Community Slack Channel** - Get an invite to chat with community members and the VoltDB team at http://chat.voltdb.com.


Licensing
====================

This program is free software distributed under the terms of the 
GNU Affero General Public License Version 3. See the accompanying 
LICENSE file for details on your rights and responsibilities with 
regards to the use and redistribution of VoltDB software.
